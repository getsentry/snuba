ARG PYTHON_VERSION=2
FROM pypy:${PYTHON_VERSION}-slim

ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update && \
    apt-get install --no-install-recommends -y curl build-essential libpcre3 libpcre3-dev liblz4-1 liblz4-dev git && \
    rm -rf /var/lib/apt/lists/* /var/cache/debconf/*-old

# pypy:2-slim binary: /usr/local/bin/pypy
# pypy:3-slim binary: /usr/local/bin/pypy3
ARG PYPY_SUFFIX
RUN ln -s /usr/local/bin/pypy${PYPY_SUFFIX} /usr/local/bin/python

ARG LIBRDKAFKA_VERSION=0.11.5
RUN cd && curl -L https://github.com/edenhill/librdkafka/archive/v${LIBRDKAFKA_VERSION}.tar.gz -O && \
    tar xf v${LIBRDKAFKA_VERSION}.tar.gz && cd librdkafka-${LIBRDKAFKA_VERSION}/ && \
    ./configure --prefix=/usr && make && PREFIX=/usr make install && \
    cd && rm -rf v${LIBRDKAFKA_VERSION}.tar.gz librdkafka-${LIBRDKAFKA_VERSION}/

RUN useradd -m -s /bin/bash snuba
WORKDIR /home/snuba

# This is required in addition to the PYTHON_VERSION ARG at the top, because
# apparently the one before FROM is not in scope here.
ARG PYTHON_VERSION=2
COPY requirements-py${PYTHON_VERSION}.txt ./
RUN pip install --no-cache-dir -r requirements-py${PYTHON_VERSION}.txt

COPY bin ./bin/
COPY snuba ./snuba/
COPY setup.py README.md ./

RUN python setup.py install && rm -rf ./build ./dist

ENV CLICKHOUSE_SERVER clickhouse-server:9000
ENV CLICKHOUSE_TABLE sentry
ENV FLASK_DEBUG 0

USER snuba

EXPOSE 1218

COPY docker_entrypoint.sh ./
ENTRYPOINT [ "./docker_entrypoint.sh" ]
CMD [ "api" ]
