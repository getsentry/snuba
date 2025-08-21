=============================
Snuba development environment
=============================

This section explains how to run snuba from source and set up a development
environment.

In order to set up Clickhouse, Redis, and Kafka, please refer to :doc:`/getstarted`.

Prerequisites
-------------
It is assumed that you have completed the steps to set up the `sentry dev environment <https://develop.sentry.dev/environment/>`_.

You will need an installation of Rust to develop Snuba. Go `here <https://rustup.rs>`_ to get one::

    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

Make sure to follow installation steps and configure your shell for cargo (Rust's build sytem and package manager).

If you are using Homebrew and a M1 Mac, ensure the development packages you've installed with Homebrew are available
by setting these environment variables::

    export CPATH=/opt/homebrew/include
    export LIBRARY_PATH=/opt/homebrew/lib

Install / Run
-------------

Clone this repo into your workspace::

    git clone git@github.com:getsentry/snuba.git

These commands set up the Python virtual environment::

    cd snuba
    devenv sync

This builds rust_snuba (it's expensive, so is kept out of `devenv sync`)::

    make install-rs-dev (one time)
    make watch-rust-snuba (or watch and rebuild)

This command starts the Snuba api, which is capable of processing queries::

    snuba api

This command starts the api and Snuba consumers to ingest
data from Kafka::

    snuba devserver

Running tests
-------------

This command runs unit and integration tests::

    devenv sync (if you have not run it already)
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
    devservices up
    docker stop snuba-snuba-1 snuba-clickhouse-1

This will get the most recent version of Sentry on master, and bring up all snuba's dependencies.

You will want to run the following Sentry tests::

    make test-acceptance
    make test-snuba
    make test-python

These tests do not use Kafka due to performance reasons. The snuba test suite does test the kafka functionality
