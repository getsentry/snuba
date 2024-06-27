=============================
Snuba development environment
=============================

This section explains how to run snuba from source and set up a development
environment.

In order to set up Clickhouse, Redis, and Kafka, please refer to :doc:`/getstarted`.

Prerequisites
-------------
`pyenv <https://github.com/pyenv/pyenv#installation>`_ must be installed on your system.
It is also assumed that you have completed the steps to set up the `sentry dev environment <https://develop.sentry.dev/environment/>`_.

If you are using Homebrew and a M1 Mac, ensure the development packages you've installed with Homebrew are available
by setting these environment variables::

    export CPATH=/opt/homebrew/include
    export LIBRARY_PATH=/opt/homebrew/lib

Install / Run
-------------

clone this repo into your workspace::

    git@github.com:getsentry/snuba.git

These commands set up the Python virtual environment::

    cd snuba
    python -m venv .venv
    source .venv/bin/activate
    pip install --upgrade pip==22.2.2
    make develop

These commands start the Snuba api, which is capable of processing queries::

    snuba api

This command instead will start the api and all the Snuba consumers to ingest
data from Kafka::

    snuba devserver

Running tests
-------------

This command runs unit and integration tests::

    make develop (if you have not run it already)
    make test

Running sentry tests against snuba
++++++++++++++++++++++++++++++++++

This section instead runs Sentry tests against a running Snuba installation

Make sure there is no snuba container already running::

    docker ps -a | grep snuba

Start your local snuba api server::

    git checkout your-snuba-branch
    source .venv/bin/activate
    snuba api

and, in another terminal::

    cd ../sentry
    git checkout master
    git pull
    sentry devservices up --exclude=snuba

This will get the most recent version of Sentry on master, and bring up all snuba's dependencies.

You will want to run the following Sentry tests::

    make test-acceptance
    make test-snuba
    make test-python

These tests do not use Kafka due to performance reasons. The snuba test suite does test the kafka functionality
