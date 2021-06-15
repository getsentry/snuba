=============================
Snuba development environment
=============================

This section explains how to run snuba from source and set up a development
environment.

In order to set up Clickhouse and Redis, please refer to :doc:`/getstarted`.

Install / Run
-------------

These commands set up the Python virtual environment::

    mkvirtualenv snuba --python=python3.8
    workon snuba
    make install-python-dependencies
    make setup-git

These commands start the Snuba api, which is capable of processing queries::

    snuba api

This command instead will start the api and all the Snuba consumers to ingest
data from Kafka::

    snuba devserver

Running tests
-------------

This command runs unit and integration tests::

    pip install -e .
    make test

This section instead runs Sentry tests against a running Snuba installation::

    workon snuba
    git checkout your-snuba-branch
    snuba api

and, in another terminal::

    workon sentry
    git checkout master
    git pull
    sentry devservices up --exclude=snuba

This will get the most recent version of Sentry on master, and bring up all snuba's dependencies.

You will want to run the following Sentry tests::

    make test-acceptance
    make test-snuba
    make test-python

These tests do not use Kafka.
