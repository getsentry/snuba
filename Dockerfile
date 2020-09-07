ARG PYTHON_VERSION=3.8
FROM python:${PYTHON_VERSION}-slim

# these are required all the way through, and removing them will cause bad things
ENV SYSTEM_DEPENDENCIES \
    libexpat1 \
    libffi6 \
    liblz4-1 \
    libpcre3

# These are temporarily needed to install things like uwsgi.
# It's installed here for better layer caching, otherwise every time
# "COPY . /usr/src/snuba" is invalidated we end up pulling these.
ENV BUILD_DEPENDENCIES \
    curl \
    gcc \
    libc6-dev \
    liblz4-dev \
    libpcre3-dev

RUN set -ex; \
    apt-get update; \
    apt-get install --no-install-recommends -y $SYSTEM_DEPENDENCIES $BUILD_DEPENDENCIES; \
    rm -rf /var/lib/apt/lists/*

# grab gosu for easy step-down from root
RUN set -x \
    && export GOSU_VERSION=1.11 \
    && fetchDeps=" \
        dirmngr \
        gnupg \
    " \
    && apt-get update && apt-get install -y --no-install-recommends $fetchDeps && rm -rf /var/lib/apt/lists/* \
    && curl -L -o /usr/local/bin/gosu "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-amd64" \
    && curl -L -o /usr/local/bin/gosu.asc "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-amd64.asc" \
    && export GNUPGHOME="$(mktemp -d)" \
    && for key in \
      B42F6819007F00F88E364FD4036A9C25BF357DD4 \
    ; do \
      gpg --batch --keyserver hkp://p80.pool.sks-keyservers.net:80 --recv-keys "$key" || \
      gpg --batch --keyserver hkp://ipv4.pool.sks-keyservers.net --recv-keys "$key" || \
      gpg --batch --keyserver hkp://pgp.mit.edu:80 --recv-keys "$key" ; \
    done \
    && gpg --batch --verify /usr/local/bin/gosu.asc /usr/local/bin/gosu \
    && gpgconf --kill all \
    && rm -r "$GNUPGHOME" /usr/local/bin/gosu.asc \
    && chmod +x /usr/local/bin/gosu \
    && gosu nobody true \
    && apt-get purge -y --auto-remove $fetchDeps

WORKDIR /usr/src/snuba
# Layer cache is pretty much invalidated here all the time,
# so try not to do anything heavy beyond here.
COPY . ./
RUN set -ex; \
    groupadd -r snuba; \
    useradd -r -g snuba snuba; \
    chown -R snuba:snuba ./

ENV PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on

RUN set -ex; \
    pip install -e .; \
    mkdir /tmp/uwsgi-dogstatsd; \
    curl -L https://github.com/DataDog/uwsgi-dogstatsd/archive/bc56a1b5e7ee9e955b7a2e60213fc61323597a78.tar.gz \
        | tar -xvz -C /tmp/uwsgi-dogstatsd --strip-components=1; \
    uwsgi --build-plugin /tmp/uwsgi-dogstatsd; \
    rm -rf /tmp/uwsgi-dogstatsd .uwsgi_plugins_builder; \
    mkdir -p /var/lib/uwsgi; \
    mv dogstatsd_plugin.so /var/lib/uwsgi/; \
    uwsgi --need-plugin=/var/lib/uwsgi/dogstatsd --help > /dev/null; \
    snuba --help; \
    apt-get purge -y --auto-remove $BUILD_DEPENDENCIES

ARG SNUBA_VERSION_SHA
ENV SNUBA_RELEASE=$SNUBA_VERSION_SHA \
    FLASK_DEBUG=0 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UWSGI_ENABLE_METRICS=true \
    UWSGI_NEED_PLUGIN=/var/lib/uwsgi/dogstatsd \
    UWSGI_STATS_PUSH=dogstatsd:127.0.0.1:8126 \
    UWSGI_DOGSTATSD_EXTRA_TAGS=service:snuba

EXPOSE 1218
ENTRYPOINT [ "./docker_entrypoint.sh" ]
CMD [ "api" ]
