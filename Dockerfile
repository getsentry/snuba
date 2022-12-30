ARG PYTHON_VERSION=3.8.13
FROM python:${PYTHON_VERSION}-slim-bullseye AS base

WORKDIR /usr/src/snuba

ENV PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on

# Install dependencies first because requirements.txt is way less likely to be changed.
COPY requirements.txt ./
RUN set -ex; \
    \
    buildDeps=' \
        gcc \
        libc6-dev \
        liblz4-dev \
        libpcre3-dev \
        wget \
        zlib1g-dev \
    '; \
    apt-get update; \
    apt-get install -y $buildDeps --no-install-recommends; \
    pip install -r requirements.txt; \
    \
    mkdir /tmp/uwsgi-dogstatsd; \
    wget -O - https://github.com/DataDog/uwsgi-dogstatsd/archive/bc56a1b5e7ee9e955b7a2e60213fc61323597a78.tar.gz \
    | tar -xvz -C /tmp/uwsgi-dogstatsd --strip-components=1; \
    uwsgi --build-plugin /tmp/uwsgi-dogstatsd; \
    rm -rf /tmp/uwsgi-dogstatsd .uwsgi_plugins_builder; \
    mkdir -p /var/lib/uwsgi; \
    mv dogstatsd_plugin.so /var/lib/uwsgi/; \
    # TODO: https://github.com/lincolnloop/pyuwsgi-wheels/pull/17
    python -c 'import os, sys; sys.setdlopenflags(sys.getdlopenflags() | os.RTLD_GLOBAL); import pyuwsgi; pyuwsgi.run()' --need-plugin=/var/lib/uwsgi/dogstatsd --help > /dev/null; \
    \
    apt-get purge -y --auto-remove $buildDeps; \
    rm -rf /var/lib/apt/lists/*;

# Install nodejs and yarn and build the admin UI
FROM base AS build_admin_ui
ENV NODE_VERSION=19
COPY ./snuba/admin ./snuba/admin
RUN set -ex; \
    apt-get update && \
    apt-get install -y curl gnupg --no-install-recommends && \
    curl -sS https://dl.yarnpkg.com/debian/pubkey.gpg | apt-key add - && \
    echo "deb https://dl.yarnpkg.com/debian/ stable main" | tee /etc/apt/sources.list.d/yarn.list && \
    curl -fsSL https://deb.nodesource.com/setup_${NODE_VERSION}.x | bash - &&\
    apt-get update && \
    apt-get install -y yarn nodejs --no-install-recommends && \
    cd snuba/admin && \
    yarn install && \
    yarn run build && \
    yarn cache clean && \
    rm -rf node_modules && \
    apt-get purge -y --auto-remove yarn curl nodejs gnupg && \
    rm -rf /var/lib/apt/lists/*

# Layer cache is pretty much invalidated here all the time,
# so try not to do anything heavy beyond here.
FROM base AS application
COPY . ./
RUN set -ex; \
    groupadd -r snuba --gid 1000; \
    useradd -r -g snuba --uid 1000 snuba; \
    chown -R snuba:snuba ./; \
    pip install -e .; \
    snuba --help;

ARG SOURCE_COMMIT
ENV SNUBA_RELEASE=$SOURCE_COMMIT \
    FLASK_DEBUG=0 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UWSGI_ENABLE_METRICS=true \
    UWSGI_NEED_PLUGIN=/var/lib/uwsgi/dogstatsd \
    UWSGI_STATS_PUSH=dogstatsd:127.0.0.1:8126 \
    UWSGI_DOGSTATSD_EXTRA_TAGS=service:snuba

USER snuba
EXPOSE 1218
ENTRYPOINT [ "./docker_entrypoint.sh" ]
CMD [ "api" ]

FROM application AS testing

USER 0
RUN pip install -r requirements-test.txt
USER snuba
