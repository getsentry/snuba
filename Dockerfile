ARG PYTHON_VERSION=3.10.13

FROM python:${PYTHON_VERSION}-slim-bookworm as build_base
WORKDIR /usr/src/snuba

ENV PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on

# requirements-build.txt is separate from requirements.txt so that Python
# dependency bumps do not cause invalidation of the Rust layer.
COPY requirements-build.txt ./

RUN set -ex; \
    \
    buildDeps=' \
        curl \
        git \
        gcc \
        libc6-dev \
        liblz4-dev \
        libpcre3-dev \
        libssl-dev \
        wget \
        zlib1g-dev \
        pkg-config \
        cmake \
        make \
        g++ \
        gnupg \
    '; \
    runtimeDeps=' \
        libjemalloc2 \
    '; \
    apt-get update; \
    apt-get install -y $buildDeps $runtimeDeps --no-install-recommends; \
    pip install -r requirements-build.txt; \
    ln -s /usr/lib/*/libjemalloc.so.2 /usr/src/snuba/libjemalloc.so.2; \
    echo "$buildDeps" > /tmp/build-deps.txt

FROM build_base AS base

# Install dependencies first because requirements.txt is way less likely to be
# changed compared to the rest of the application.
COPY requirements.txt ./
RUN set -ex; \
    \
    pip install -r requirements.txt; \
    mkdir /tmp/uwsgi-dogstatsd; \
    wget -O - https://github.com/DataDog/uwsgi-dogstatsd/archive/bc56a1b5e7ee9e955b7a2e60213fc61323597a78.tar.gz \
    | tar -xvz -C /tmp/uwsgi-dogstatsd --strip-components=1; \
    uwsgi --build-plugin /tmp/uwsgi-dogstatsd; \
    rm -rf /tmp/uwsgi-dogstatsd .uwsgi_plugins_builder; \
    mkdir -p /var/lib/uwsgi; \
    mv dogstatsd_plugin.so /var/lib/uwsgi/; \
    # TODO: https://github.com/lincolnloop/pyuwsgi-wheels/pull/17
    python -c 'import os, sys; sys.setdlopenflags(sys.getdlopenflags() | os.RTLD_GLOBAL); import pyuwsgi; pyuwsgi.run()' --need-plugin=/var/lib/uwsgi/dogstatsd --help > /dev/null

# We assume that compared to snuba codebase, the Rust consumer is the least likely to get
# changed. We do need requirements.txt installed though, so we cannot use the
# official Rust docker images and rather have to install Rust into the existing
# Python image.
# We could choose to install maturin into the Rust docker image instead of the
# entirety of requirements.txt, but we are not yet confident that the Python
# library we build is portable across Python versions.
# There are optimizations we can make in the future to split building of Rust
# dependencies from building the Rust source code, see Relay Dockerfile.

FROM build_base AS build_rust_snuba_base
ARG RUST_TOOLCHAIN=1.74.1

RUN set -ex; \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --default-toolchain $RUST_TOOLCHAIN  --profile minimal -y; \
    # use git CLI to avoid OOM on ARM64
    echo '[net]' > ~/.cargo/config; \
    echo 'git-fetch-with-cli = true' >> ~/.cargo/config; \
    echo '[registries.crates-io]' >> ~/.cargo/config; \
    echo 'protocol = "sparse"' >> ~/.cargo/config

ENV PATH="/root/.cargo/bin/:${PATH}"

FROM build_rust_snuba_base AS build_rust_snuba_deps

COPY ./rust_snuba/Cargo.toml ./rust_snuba/Cargo.toml
COPY ./rust_snuba/Cargo.lock ./rust_snuba/Cargo.lock
COPY ./scripts/rust-dummy-build.sh ./scripts/rust-dummy-build.sh

RUN set -ex; \
    sh scripts/rust-dummy-build.sh; \
    cd ./rust_snuba/; \
    maturin build --release --compatibility linux --locked

FROM build_rust_snuba_base AS build_rust_snuba
COPY ./rust_snuba/ ./rust_snuba/
COPY --from=build_rust_snuba_deps /usr/src/snuba/rust_snuba/target/ ./rust_snuba/target/
COPY --from=build_rust_snuba_deps /root/.cargo/ /root/.cargo/
RUN set -ex; \
    cd ./rust_snuba/; \
    maturin build --release --compatibility linux --locked

# Install nodejs and yarn and build the admin UI
FROM build_base AS build_admin_ui
ENV NODE_VERSION=19

COPY ./snuba/admin ./snuba/admin
RUN set -ex; \
    mkdir -p snuba/admin/dist/; \
    curl -sS https://dl.yarnpkg.com/debian/pubkey.gpg | apt-key add - && \
    echo "deb https://dl.yarnpkg.com/debian/ stable main" | tee /etc/apt/sources.list.d/yarn.list && \
    curl -SLO https://deb.nodesource.com/nsolid_setup_deb.sh | bash -s -- ${NODE_VERSION} &&\
    apt-get update && \
    apt-get install -y yarn nodejs --no-install-recommends && \
    cd snuba/admin && \
    yarn install && \
    yarn run build

# Layer cache is pretty much invalidated here all the time,
# so try not to do anything heavy beyond here.
FROM base AS application_base
COPY . ./
COPY --from=build_rust_snuba /usr/src/snuba/rust_snuba/target/wheels/ /tmp/rust_wheels/
COPY --from=build_admin_ui /usr/src/snuba/snuba/admin/dist/ ./snuba/admin/dist/
RUN set -ex; \
    groupadd -r snuba --gid 1000; \
    useradd -r -g snuba --uid 1000 snuba; \
    chown -R snuba:snuba ./; \
    # Ensure that we are always importing the installed rust_snuba wheel, and not the
    # (basically empty) rust_snuba folder
    rm -rf ./rust_snuba/; \
    [ -z "`find /tmp/rust_wheels -type f`" ] || pip install /tmp/rust_wheels/*; \
    rm -rf /tmp/rust_wheels/; \
    pip install -e .; \
    snuba --help

ARG SOURCE_COMMIT
ENV LD_PRELOAD=/usr/src/snuba/libjemalloc.so.2 \
    SNUBA_RELEASE=$SOURCE_COMMIT \
    FLASK_DEBUG=0 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UWSGI_ENABLE_METRICS=true \
    UWSGI_NEED_PLUGIN=/var/lib/uwsgi/dogstatsd \
    UWSGI_STATS_PUSH=dogstatsd:127.0.0.1:8126 \
    UWSGI_DOGSTATSD_EXTRA_TAGS=service:snuba

USER snuba
EXPOSE 1218 1219
ENTRYPOINT [ "./docker_entrypoint.sh" ]
CMD [ "api" ]

FROM application_base as application
USER 0
RUN set -ex; \
    apt-get purge -y --auto-remove $(cat /tmp/build-deps.txt); \
    rm /tmp/build-deps.txt; \
    rm -rf /var/lib/apt/lists/*;
USER snuba

FROM application_base AS testing

USER 0
RUN pip install -r requirements-test.txt

COPY ./rust_snuba/ ./rust_snuba/
# re-"install" rust for the testing image
COPY --from=build_rust_snuba /root/.cargo/ /root/.cargo/
COPY --from=build_rust_snuba /root/.rustup/ /root/.rustup/
ENV PATH="${PATH}:/root/.cargo/bin/"
USER snuba
