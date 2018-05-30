FROM pypy:2-slim

ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update && \
    apt-get install --no-install-recommends -y curl build-essential libpcre3 libpcre3-dev liblz4-1 liblz4-dev && \
    rm -rf /var/lib/apt/lists/* /var/cache/debconf/*-old
RUN ln -s /usr/local/bin/pypy /usr/local/bin/python

RUN cd && curl -L https://github.com/edenhill/librdkafka/archive/v0.11.4.tar.gz -O && \
    tar xf v0.11.4.tar.gz && cd librdkafka-0.11.4/ && \
    ./configure --prefix=/usr && make && PREFIX=/usr make install && \
    cd && rm -rf v0.11.4.tar.gz librdkafka-0.11.4/

RUN useradd -m -s /bin/bash snuba
WORKDIR /home/snuba

COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt

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
