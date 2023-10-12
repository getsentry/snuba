ARG PYTHON_VERSION=3.8.18

FROM python:${PYTHON_VERSION}-slim-bullseye as build_base
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
    apt-get update; \
    apt-get install -y $buildDeps --no-install-recommends; \
    pip install -r requirements-build.txt; \
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

FROM build_base AS build_rust_snuba
ARG RUST_TOOLCHAIN=1.72
ARG SHOULD_BUILD_RUST=true

COPY ./rust_snuba/ ./rust_snuba/
RUN set -ex; \
    cd ./rust_snuba/; \
    mkdir -p ./target/wheels/; \
    [ "$SHOULD_BUILD_RUST" = "true" ] || exit 0; \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --default-toolchain $RUST_TOOLCHAIN  --profile minimal -y; \
    export PATH="$HOME/.cargo/bin/:$PATH"; \
    # use git CLI to avoid OOM on ARM64
    echo '[net]' > ~/.cargo/config; \
    echo 'git-fetch-with-cli = true' >> ~/.cargo/config; \
    echo '[registries.crates-io]' >> ~/.cargo/config; \
    echo 'protocol = "sparse"' >> ~/.cargo/config; \
    maturin build --release --compatibility linux --locked --strip

# Install nodejs and yarn and build the admin UI
FROM build_base AS build_admin_ui
ARG SHOULD_BUILD_ADMIN_UI=true
ENV NODE_VERSION=19

COPY ./snuba/admin ./snuba/admin
RUN set -ex; \
    mkdir -p snuba/admin/dist/; \
    [ "$SHOULD_BUILD_ADMIN_UI" = "true" ] || exit 0; \
    curl -sS https://dl.yarnpkg.com/debian/pubkey.gpg | apt-key add - && \
    echo "deb https://dl.yarnpkg.com/debian/ stable main" | tee /etc/apt/sources.list.d/yarn.list && \
    curl -fsSL https://deb.nodesource.com/setup_${NODE_VERSION}.x | bash - &&\
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

ARG SOURCE_COMMIT
ENV SNUBA_RELEASE=$SOURCE_COMMIT \
    FLASK_DEBUG=0 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UWSGI_ENABLE_METRICS=true \
    UWSGI_NEED_PLUGIN=/var/lib/uwsgi/dogstatsd \
    UWSGI_STATS_PUSH=dogstatsd:127.0.0.1:8126 \
    UWSGI_DOGSTATSD_EXTRA_TAGS=service:snuba


FROM application_base AS testing

USER 0
RUN pip install -r requirements-test.txt

ARG RUST_TOOLCHAIN=1.72
COPY ./rust_snuba/ ./rust_snuba/
RUN bash -c "set -o pipefail && curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --default-toolchain $RUST_TOOLCHAIN  --profile minimal -y"
ENV PATH="${PATH}:/root/.cargo/bin/"
USER snuba
