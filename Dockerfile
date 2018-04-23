FROM python:2-slim

ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update && \
    apt-get install --no-install-recommends -y build-essential libpcre3 libpcre3-dev && \
    rm -rf /var/lib/apt/lists/* /var/cache/debconf/*-old

RUN useradd -m -s /bin/bash snuba
WORKDIR /home/snuba

COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt

COPY bin ./bin/
COPY snuba ./snuba/
COPY setup.py README.md ./

RUN python setup.py install && rm -rf ./build ./dist

ENV CLICKHOUSE_SERVERS clickhouse-server:9000
ENV CLICKHOUSE_TABLE sentry
ENV FLASK_DEBUG 0

USER snuba

EXPOSE 1218

COPY docker_entrypoint.sh ./
ENTRYPOINT [ "./docker_entrypoint.sh" ]
CMD [ "api" ]
