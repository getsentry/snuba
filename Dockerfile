FROM python:2-slim

RUN groupadd -r snuba && useradd -r -g snuba snuba

RUN mkdir -p /usr/src/snuba
WORKDIR /usr/src/snuba

ENV PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PYTHONDONTWRITEBYTECODE=1

RUN set -ex; \
    apt-get update; \
    apt-get install --no-install-recommends -y \
        libexpat1 \
        libffi6 \
        libpcre3 \
        liblz4-1 \
    ; \
    rm -rf /var/lib/apt/lists/*

# grab gosu for easy step-down from root
ENV GOSU_VERSION 1.10
RUN set -ex; \
    \
    buildDeps=' \
        dirmngr \
        gnupg \
        wget \
    '; \
    apt-get update; \
    apt-get install -y --no-install-recommends $buildDeps; \
    rm -rf /var/lib/apt/lists/*; \
    \
    wget -O /usr/local/bin/gosu "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-$(dpkg --print-architecture)"; \
    wget -O /usr/local/bin/gosu.asc "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-$(dpkg --print-architecture).asc"; \
    export GNUPGHOME="$(mktemp -d)"; \
    gpg --batch --keyserver ipv4.pool.sks-keyservers.net --recv-keys B42F6819007F00F88E364FD4036A9C25BF357DD4; \
    gpg --batch --verify /usr/local/bin/gosu.asc /usr/local/bin/gosu; \
    rm -rf "$GNUPGHOME" /usr/local/bin/gosu.asc; \
    chmod +x /usr/local/bin/gosu; \
    gosu nobody true; \
    \
    apt-get purge -y --auto-remove $buildDeps

RUN set -ex; \
    LIBRDKAFKA_VERSION=0.11.5; \
    \
    buildDeps=' \
        make \
        gcc \
        g++ \
        libc6-dev \
        liblz4-dev \
        wget \
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

COPY snuba ./snuba/
COPY setup.py README.md MANIFEST.in ./

RUN chown -R snuba:snuba /usr/src/snuba/

# Install PyPy at /pypy, for running the consumer code. Note that PyPy is built
# against libssl1.0.0, so this is required for using the SSL module, which is
# required to bootstrap pip. Since this is a short term stopgap it seemed better
# than building PyPy ourselves.
RUN set -ex; \
    \
    buildDeps=' \
        bzip2 \
        gcc \
        libc6-dev \
        liblz4-dev \
        libpcre3-dev \
        wget \
    '; \
    apt-get update; \
    apt-get install -y $buildDeps --no-install-recommends; \
    rm -rf /var/lib/apt/lists/*; \
    \
    wget https://bitbucket.org/pypy/pypy/downloads/pypy2-v6.0.0-linux64.tar.bz2; \
    [ "$(sha256sum pypy2-v6.0.0-linux64.tar.bz2)" = '6cbf942ba7c90f504d8d6a2e45d4244e3bf146c8722d64e9410b85eac6b5af67  pypy2-v6.0.0-linux64.tar.bz2' ]; \
    tar xf pypy2-v6.0.0-linux64.tar.bz2; \
    rm -rf pypy2-v6.0.0-linux64.tar.bz2; \
    mv pypy2-v6.0.0-linux64 /pypy; \
    wget http://security.debian.org/debian-security/pool/updates/main/o/openssl/libssl1.0.0_1.0.1t-1+deb7u4_amd64.deb; \
    DEBIAN_FRONTEND=noninteractive dpkg -i libssl1.0.0_1.0.1t-1+deb7u4_amd64.deb; \
    rm -rf libssl1.0.0_1.0.1t-1+deb7u4_amd64.deb; \
    wget https://bootstrap.pypa.io/get-pip.py; \
    /pypy/bin/pypy get-pip.py; \
    rm -rf get-pip.py; \
    \
    /pypy/bin/pip install -e . && /pypy/bin/snuba --help; \
    \
    apt-get purge -y --auto-remove $buildDeps

RUN set -ex; \
    \
    buildDeps=' \
        gcc \
        libc6-dev \
        liblz4-dev \
        libpcre3-dev \
    '; \
    apt-get update; \
    apt-get install -y $buildDeps --no-install-recommends; \
    rm -rf /var/lib/apt/lists/*; \
    \
    pip install -e . && snuba --help; \
    \
    apt-get purge -y --auto-remove $buildDeps

ENV CLICKHOUSE_SERVER clickhouse-server:9000
ENV CLICKHOUSE_TABLE sentry
ENV FLASK_DEBUG 0
ARG GIT_SHA
ENV SNUBA_RELEASE=$GIT_SHA

EXPOSE 1218

COPY docker_entrypoint.sh ./
ENTRYPOINT [ "./docker_entrypoint.sh" ]
CMD [ "api" ]
