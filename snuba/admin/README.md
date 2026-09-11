# Snuba Admin

## Prerequisites

You must have node and yarn installed (recommended: [volta](https://volta.sh/)). To do so, run the following commands from the root of the repository:

```shell
volta install node
volta install yarn
```

## Running the Admin UI

From the Snuba repository, run the following command to start Snuba's dependencies (ClickHouse, Redis, Kafka):

```shell
devservices up
```

Then, build rust_snuba by following the documentation for [running Snuba locally](/docs/source/contributing/environment.rst).

To build the app:

```shell
make build-admin
```

To run the app locally:

```shell
# assuming you're in the venv
snuba admin
```

The server should be running on <http://127.0.0.1:1219>

## Developing the JavaScript

To start the yarn debug server and live reload your javascript changes.

```shell
make watch-admin
```

> [!NOTE]
> If you change environment variables, you will have to restart the server.

## Releasing New JavaScript

The admin tool is automatically built as part of our normal CI flow.
