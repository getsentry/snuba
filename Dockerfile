ARG PYTHON_VERSION=2
FROM pypy:${PYTHON_VERSION}-slim

# pypy:2-slim binary: /usr/local/bin/pypy
# pypy:3-slim binary: /usr/local/bin/pypy3
ARG PYPY_SUFFIX
RUN ln -s /usr/local/bin/pypy${PYPY_SUFFIX} /usr/local/bin/python

RUN groupadd -r snuba && useradd -r -g snuba snuba

RUN mkdir -p /usr/src/snuba
WORKDIR /usr/src/snuba

ENV PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PYTHONDONTWRITEBYTECODE=1

RUN set -ex; \
    apt-get update; \
    apt-get install --no-install-recommends -y \
        gcc \
        libpcre3 \
        libpcre3-dev \
        liblz4-1 \
    ; \
    rm -rf /var/lib/apt/lists/*

# grab gosu for easy step-down from root
ENV GOSU_VERSION 1.10
RUN set -ex; \
    apt-get update && apt-get install -y --no-install-recommends wget && rm -rf /var/lib/apt/lists/*; \
    wget -O /usr/local/bin/gosu "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-$(dpkg --print-architecture)"; \
    wget -O /usr/local/bin/gosu.asc "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-$(dpkg --print-architecture).asc"; \
    export GNUPGHOME="$(mktemp -d)"; \
    gpg --keyserver ha.pool.sks-keyservers.net --recv-keys B42F6819007F00F88E364FD4036A9C25BF357DD4; \
    gpg --batch --verify /usr/local/bin/gosu.asc /usr/local/bin/gosu; \
    rm -r "$GNUPGHOME" /usr/local/bin/gosu.asc; \
    chmod +x /usr/local/bin/gosu; \
    gosu nobody true; \
    apt-get purge -y --auto-remove wget

RUN set -ex; \
    LIBRDKAFKA_VERSION=0.11.5; \
    \
    buildDeps=' \
        wget \
        make \
        g++ \
        liblz4-dev \
    '; \
    apt-get update; \
    apt-get install -y $buildDeps --no-install-recommends; \
    rm -rf /var/lib/apt/lists/*; \
    \
    mkdir -p /usr/src/librdkafka; \
    cd /usr/src/librdkafka; \
    wget -O v${LIBRDKAFKA_VERSION}.tar.gz https://github.com/edenhill/librdkafka/archive/v${LIBRDKAFKA_VERSION}.tar.gz; \
    tar xf v${LIBRDKAFKA_VERSION}.tar.gz --strip-components=1; \
    ./configure --prefix=/usr; \
    make; \
    PREFIX=/usr make install; \
    rm -r /usr/src/librdkafka; \
    \
    apt-get purge -y --auto-remove $buildDeps

# This is required in addition to the PYTHON_VERSION ARG at the top, because
# apparently the one before FROM is not in scope here.
RUN set -ex; \
    \
    buildDeps=' \
        liblz4-dev \
    '; \
    apt-get update; \
    apt-get install -y $buildDeps --no-install-recommends; \
    rm -rf /var/lib/apt/lists/*; \
    \
    apt-get purge -y --auto-remove $buildDeps

COPY snuba ./snuba/
COPY setup.py README.md ./

RUN chown -R snuba:snuba /usr/src/snuba/

RUN pip install -e . --process-dependency-links && snuba --help

ENV CLICKHOUSE_SERVER clickhouse-server:9000
ENV CLICKHOUSE_TABLE sentry
ENV FLASK_DEBUG 0


EXPOSE 1218

COPY docker_entrypoint.sh ./
ENTRYPOINT [ "./docker_entrypoint.sh" ]
CMD [ "api" ]
