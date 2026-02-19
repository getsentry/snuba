ARG PYTHON_VERSION=3.13.12

FROM python:${PYTHON_VERSION}-slim-bookworm AS build_base

WORKDIR /usr/src/snuba

ENV PATH="/.venv/bin:$PATH" UV_PROJECT_ENVIRONMENT=/.venv \
		PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1 \
    UV_COMPILE_BYTECODE=1 UV_NO_CACHE=1

RUN python3 -m pip install \
		--index-url 'https://pypi.devinfra.sentry.io/simple' 'uv==0.8.2'

# We don't want uv-managed python, we want to use python from the image.
# We only want to use uv to manage dependencies.
RUN python3 -m venv "$UV_PROJECT_ENVIRONMENT"

RUN set -ex; \
    \
    buildDeps=' \
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
        protobuf-compiler \
    '; \
    runtimeDeps=' \
        curl \
        libjemalloc2 \
        gdb \
        heaptrack \
    '; \
    apt-get update; \
    apt-get install -y $buildDeps $runtimeDeps --no-install-recommends; \
    ln -s /usr/lib/*/libjemalloc.so.2 /usr/src/snuba/libjemalloc.so.2; \
    echo "$buildDeps" > /tmp/build-deps.txt

FROM build_base AS base

COPY pyproject.toml uv.lock ./
RUN set -ex; \
    \
    uv sync --no-dev --frozen --no-install-project --no-install-workspace

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

RUN set -ex; \
    # do not install any toolchain, as rust_snuba/rust-toolchain.toml defines that for us
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --default-toolchain none -y; \
    # use git CLI to avoid OOM on ARM64
    echo '[net]' > ~/.cargo/config; \
    echo 'git-fetch-with-cli = true' >> ~/.cargo/config; \
    echo '[registries.crates-io]' >> ~/.cargo/config; \
    echo 'protocol = "sparse"' >> ~/.cargo/config

ENV PATH="/root/.cargo/bin/:${PATH}"

FROM build_rust_snuba_base AS build_rust_snuba_deps

COPY ./rust_snuba/Cargo.toml ./rust_snuba/Cargo.toml
COPY ./rust_snuba/rust-toolchain.toml ./rust_snuba/rust-toolchain.toml
COPY ./rust_snuba/Cargo.lock ./rust_snuba/Cargo.lock
COPY ./scripts/rust-dummy-build.sh ./scripts/rust-dummy-build.sh

RUN set -ex; \
    sh scripts/rust-dummy-build.sh; \
    cd ./rust_snuba/; \
    rustup show active-toolchain || rustup toolchain install

FROM build_rust_snuba_deps AS build_rust_snuba
COPY ./rust_snuba/ ./rust_snuba/
COPY --from=build_rust_snuba_deps /usr/src/snuba/rust_snuba/target/ ./rust_snuba/target/
COPY --from=build_rust_snuba_deps /root/.cargo/ /root/.cargo/
RUN set -ex; \
    cd ./rust_snuba/; \
    uvx maturin build --release --compatibility linux --locked; \
    rm -rf /root/.rustup/toolchains/*/share/doc

# Install nodejs and yarn and build the admin UI
FROM build_base AS build_admin_ui
ENV NODE_VERSION=20

COPY ./snuba/admin ./snuba/admin
RUN set -ex; \
    mkdir -p snuba/admin/dist/; \
    curl -fsSL https://deb.nodesource.com/setup_${NODE_VERSION}.x | bash - && \
    apt-get install -y nodejs --no-install-recommends && \
    npm install -g yarn && \
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
    uv sync --no-dev --frozen --no-install-package rust_snuba; \
    # Ensure that we are always importing the installed rust_snuba wheel, and not the
    # (basically empty) rust_snuba folder
    rm -rf ./rust_snuba/; \
    [ -z "`find /tmp/rust_wheels -type f`" ] || uv pip install /tmp/rust_wheels/*; \
    rm -rf /tmp/rust_wheels/; \
    snuba --help

ARG SOURCE_COMMIT
ENV LD_PRELOAD=/usr/src/snuba/libjemalloc.so.2 \
    SNUBA_RELEASE=$SOURCE_COMMIT \
    FLASK_DEBUG=0 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

USER snuba
EXPOSE 1218 1219
ENTRYPOINT [ "./docker_entrypoint.sh" ]
CMD [ "api" ]

FROM application_base AS application
USER 0
RUN set -ex; \
    apt-get purge -y --auto-remove $(cat /tmp/build-deps.txt); \
    rm /tmp/build-deps.txt; \
    rm -rf /var/lib/apt/lists/*;
USER snuba

FROM application_base AS testing

USER 0
COPY ./rust_snuba/ ./rust_snuba/
# re-"install" rust for the testing image
COPY --from=build_rust_snuba /root/.cargo/ /root/.cargo/
COPY --from=build_rust_snuba /root/.rustup/ /root/.rustup/

COPY --from=build_rust_snuba /usr/src/snuba/rust_snuba/target/wheels/ /tmp/rust_wheels/
RUN set -ex; \
    # we need to resync, this time with dev dependencies
    uv sync --frozen --no-install-package rust_snuba; \
    # this will uninstall the rust wheel so we need to reinstall again
    uv pip install /tmp/rust_wheels/*; \
    rm -rf /tmp/rust_wheels/; \
    snuba --help

ENV PATH="${PATH}:/root/.cargo/bin/"
USER snuba
