=============================
Snuba development environment
=============================

This section explains how to run Snuba from source. To set up ClickHouse,
Redis, and Kafka, see :doc:`/getstarted`.

Prerequisites
-------------

Set up the `Sentry development environment <https://develop.sentry.dev/environment/>`_
first.

Install ``uv``. ``devenv sync`` no longer installs it::

    brew install uv

If you use Homebrew on an Apple Silicon Mac, make the Homebrew development
packages visible to the Rust build::

    export CPATH=/opt/homebrew/include
    export LIBRARY_PATH=/opt/homebrew/lib

Install and run
---------------

Clone the repository::

    git clone git@github.com:getsentry/snuba.git
    cd snuba

Set up the environment. ``direnv allow`` puts ``.venv`` and ``rustup`` on your
``PATH``. ``devenv sync`` installs ``rustup``, ``cmake``, and ``protobuf`` from
``Brewfile``, syncs the Python virtual environment, and installs the
pre-commit hooks::

    direnv allow
    devenv sync

Build ``rust_snuba``. ``devenv sync`` skips it because the build is slow::

    make install-rs-dev

To rebuild ``rust_snuba`` on every change, run ``make watch-rust-snuba`` instead.

Start the Snuba API, which serves queries::

    snuba api

Start the API and the consumers that ingest data from Kafka::

    snuba devserver

Run tests
---------

Run the Python unit and integration tests::

    make test

Run the Rust tests::

    make test-rust

Run Sentry tests against Snuba
++++++++++++++++++++++++++++++

Make sure no Snuba container is running::

    docker ps -a | grep snuba

Start your local Snuba API::

    git checkout your-snuba-branch
    snuba api

In another terminal, update Sentry and start Snuba's dependencies::

    cd ../sentry
    git checkout master
    git pull
    devservices up
    devservices toggle snuba local

Then run the Sentry tests that cover your change. For example, the Sentry tests
under ``tests/snuba/`` query Snuba::

    .venv/bin/pytest tests/snuba

These tests do not use Kafka, for performance. The Snuba test suite covers
the Kafka paths.
