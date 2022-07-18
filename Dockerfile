ARG PYTHON_VERSION=3.8.13
FROM python:${PYTHON_VERSION}-slim-bullseye AS application

# these are required all the way through, and removing them will cause bad things
RUN set -ex; \
    apt-get update; \
    apt-get install --no-install-recommends -y \
        libexpat1 \
        liblz4-1 \
        libpcre3 \
    ; \
    rm -rf /var/lib/apt/lists/*

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
    # Since there's no confluent-kafka wheel for aarch64, remove when there is
    [ $(uname -m) = "aarch64" ] && apt-get install -y librdkafka-dev --no-install-recommends; \
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
