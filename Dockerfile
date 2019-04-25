FROM python:2-slim

RUN groupadd -r snuba && useradd -r -g snuba snuba

RUN mkdir -p /usr/src/snuba
WORKDIR /usr/src/snuba

ENV PIP_DISABLE_PIP_VERSION_CHECK=on \
    PYTHONDONTWRITEBYTECODE=1

# these are required all the way through, and removing them will cause bad things
RUN set -ex; \
    apt-get update; \
    apt-get install --no-install-recommends -y \
        libexpat1 \
        libffi6 \
        liblz4-1 \
        libpcre3 \
    ; \
    rm -rf /var/lib/apt/lists/*

# grab gosu for easy step-down from root
ENV GOSU_VERSION=1.11
RUN set -ex; \
    \
    LIBRDKAFKA_VERSION=0.11.5; \
    buildDeps=' \
        bzip2 \
        dirmngr \
        git \
        g++ \
        gcc \
        libc6-dev \
        liblz4-dev \
        libpcre3-dev \
        gnupg \
        make \
        wget \
    '; \
    apt-get update; \
    apt-get install -y --no-install-recommends libexpat1 libffi6 liblz4-1 libpcre3 $buildDeps; \
    rm -rf /var/lib/apt/lists/*; \
    \
    wget -O /usr/local/bin/gosu "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-$(dpkg --print-architecture)"; \
    wget -O /usr/local/bin/gosu.asc "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-$(dpkg --print-architecture).asc"; \
    export GNUPGHOME="$(mktemp -d)"; \
    for key in \
      B42F6819007F00F88E364FD4036A9C25BF357DD4 \
    ; do \
      gpg --batch --keyserver hkp://p80.pool.sks-keyservers.net:80 --recv-keys "$key" || \
      gpg --batch --keyserver hkp://ipv4.pool.sks-keyservers.net --recv-keys "$key" || \
      gpg --batch --keyserver hkp://pgp.mit.edu:80 --recv-keys "$key" ; \
    done; \
    gpg --batch --verify /usr/local/bin/gosu.asc /usr/local/bin/gosu; \
    rm -rf "$GNUPGHOME" /usr/local/bin/gosu.asc; \
    chmod +x /usr/local/bin/gosu; \
    gosu nobody true; \
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
# Install PyPy at /pypy, for running the consumer code. Note that PyPy is built
# against libssl1.0.0, so this is required for using the SSL module, which is
# required to bootstrap pip. Since this is a short term stopgap it seemed better
# than building PyPy ourselves.
    cd ; \
    wget https://bitbucket.org/pypy/pypy/downloads/pypy2-v6.0.0-linux64.tar.bz2; \
    [ "$(sha256sum pypy2-v6.0.0-linux64.tar.bz2)" = '6cbf942ba7c90f504d8d6a2e45d4244e3bf146c8722d64e9410b85eac6b5af67  pypy2-v6.0.0-linux64.tar.bz2' ]; \
    tar xf pypy2-v6.0.0-linux64.tar.bz2; \
    rm -rf pypy2-v6.0.0-linux64.tar.bz2; \
    mv pypy2-v6.0.0-linux64 /pypy; \
    wget http://security.debian.org/debian-security/pool/updates/main/o/openssl/libssl1.0.0_1.0.1t-1+deb8u11_amd64.deb; \
    DEBIAN_FRONTEND=noninteractive dpkg -i libssl1.0.0_1.0.1t-1+deb8u11_amd64.deb; \
    rm -rf libssl1.0.0_1.0.1t-1+deb8u11_amd64.deb; \
    wget https://bootstrap.pypa.io/get-pip.py; \
    /pypy/bin/pypy get-pip.py; \
    rm -rf get-pip.py; \
    \
    apt-get purge -y --auto-remove $buildDeps

COPY snuba ./snuba/
COPY setup.py Makefile README.md MANIFEST.in ./

RUN chown -R snuba:snuba /usr/src/snuba/

RUN set -ex; \
    \
    buildDeps=' \
        git \
        gcc \
        libc6-dev \
        liblz4-dev \
        libpcre3-dev \
        make \
    '; \
    apt-get update; \
    apt-get install -y $buildDeps --no-install-recommends; \
    rm -rf /var/lib/apt/lists/*; \
    \
    make install-python-dependencies; \
    snuba --help; \
    PATH=/pypy/bin:$PATH make install-python-dependencies; \
    /pypy/bin/snuba --help; \
    \
    rm -rf ~/.cache/pip; \
    apt-get purge -y --auto-remove $buildDeps

ENV CLICKHOUSE_SERVER clickhouse-server:9000
ENV FLASK_DEBUG 0
ARG SNUBA_VERSION_SHA
ENV SNUBA_RELEASE=$SNUBA_VERSION_SHA

EXPOSE 1218

COPY docker_entrypoint.sh ./
ENTRYPOINT [ "./docker_entrypoint.sh" ]
CMD [ "api" ]
