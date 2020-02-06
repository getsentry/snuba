FROM python:3.7-slim

RUN groupadd -r snuba && useradd -r -g snuba snuba

RUN mkdir -p /usr/src/snuba
WORKDIR /usr/src/snuba

ENV PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
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
    apt-get install -y --no-install-recommends multiarch-support libtinfo5 libexpat1 libffi6 liblz4-1 libpcre3 $buildDeps; \
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
    apt-get purge -y --auto-remove $buildDeps

COPY . /usr/src/snuba

RUN chown -R snuba:snuba /usr/src/snuba/

RUN set -ex; \
    \
    buildDeps=' \
        git \
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
    pip install -e .; \
    mkdir /tmp/uwsgi-dogstatsd; \
    wget -O - https://github.com/DataDog/uwsgi-dogstatsd/archive/bc56a1b5e7ee9e955b7a2e60213fc61323597a78.tar.gz \
        | tar -xvz -C /tmp/uwsgi-dogstatsd --strip-components=1; \
    uwsgi --build-plugin /tmp/uwsgi-dogstatsd; \
    rm -rf /tmp/uwsgi-dogstatsd .uwsgi_plugins_builder; \
    mkdir -p /var/lib/uwsgi; \
    mv dogstatsd_plugin.so /var/lib/uwsgi/; \
    uwsgi --need-plugin=/var/lib/uwsgi/dogstatsd --help > /dev/null; \
    snuba --help; \
    \
    apt-get purge -y --auto-remove $buildDeps

ARG SNUBA_VERSION_SHA
ENV SNUBA_RELEASE=$SNUBA_VERSION_SHA \
    FLASK_DEBUG=0 \
    PYTHONUNBUFFERED=1 \
    UWSGI_ENABLE_METRICS=true \
    UWSGI_NEED_PLUGIN=/var/lib/uwsgi/dogstatsd \
    UWSGI_STATS_PUSH=dogstatsd:127.0.0.1:8126 \
    UWSGI_DOGSTATSD_EXTRA_TAGS=service:snuba


EXPOSE 1218

COPY docker_entrypoint.sh ./
ENTRYPOINT [ "./docker_entrypoint.sh" ]
CMD [ "api" ]
