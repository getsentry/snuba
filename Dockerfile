ARG PYTHON_VERSION=3.8
FROM python:${PYTHON_VERSION}-slim AS application

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

ENV GOSU_VERSION=1.12 \
    GOSU_SHA256=0f25a21cf64e58078057adc78f38705163c1d564a959ff30a891c31917011a54

RUN set -x \
    && buildDeps=" \
      wget \
    " \
    && apt-get update && apt-get install -y --no-install-recommends $buildDeps \
    && rm -rf /var/lib/apt/lists/* \
    # grab gosu for easy step-down from root
    && wget -O /usr/local/bin/gosu "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-amd64" \
    && echo "$GOSU_SHA256 /usr/local/bin/gosu" | sha256sum --check --status \
    && chmod +x /usr/local/bin/gosu \
    && apt-get purge -y --auto-remove $buildDeps

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
    '; \
    apt-get update; \
    apt-get install -y $buildDeps --no-install-recommends; \
    \
    pip install -r requirements.txt; \
    \
    mkdir /tmp/uwsgi-dogstatsd; \
    wget -O - https://github.com/DataDog/uwsgi-dogstatsd/archive/bc56a1b5e7ee9e955b7a2e60213fc61323597a78.tar.gz \
    | tar -xvz -C /tmp/uwsgi-dogstatsd --strip-components=1; \
    uwsgi --build-plugin /tmp/uwsgi-dogstatsd; \
    rm -rf /tmp/uwsgi-dogstatsd .uwsgi_plugins_builder; \
    mkdir -p /var/lib/uwsgi; \
    mv dogstatsd_plugin.so /var/lib/uwsgi/; \
    uwsgi --need-plugin=/var/lib/uwsgi/dogstatsd --help > /dev/null; \
    \
    apt-get purge -y --auto-remove $buildDeps; \
    rm -rf /var/lib/apt/lists/*;

# Layer cache is pretty much invalidated here all the time,
# so try not to do anything heavy beyond here.
COPY . ./
RUN set -ex; \
    groupadd -r snuba; \
    useradd -r -g snuba snuba; \
    chown -R snuba:snuba ./; \
    pip install -e .; \
    snuba --help;

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

FROM application AS testing

RUN pip install -r requirements-test.txt
