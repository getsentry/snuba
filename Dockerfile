FROM python:2-slim

WORKDIR /usr/src/app

ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update && \
    apt-get install --no-install-recommends -y build-essential && \
    rm -rf /var/lib/apt/lists/* /var/cache/debconf/*-old

COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt

COPY bin ./bin/
COPY snuba ./snuba/
COPY setup.py README.md ./

RUN python setup.py install && rm -rf ./build ./dist

EXPOSE 8000

CMD [ "gunicorn", "snuba.api:app", "-b", "0.0.0.0:8000" ]
