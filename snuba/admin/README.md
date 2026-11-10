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

## Agent query endpoint

`POST /agent/query` runs read-only SQL for automation clients such as Junior. It
uses the same validation as the System Queries and Querylog tools and never
runs in sudo or clusterless mode. Responses are capped at 1000 rows.

```json
{"target": "system", "storage": "errors", "sql": "SELECT count() FROM system.parts"}
{"target": "querylog", "sql": "SELECT count() FROM querylog_local"}
```

For `system`, `host` and `port` are optional and default to the storage's
query node. List storages and nodes with `GET /clickhouse_nodes`.

Clients call it through IAP as a Google service account. Bind the service
account to `roles/AgentQueries` in the IAM policy with a
`serviceAccount:<email>` member. A bound service account gets only its bound
roles, not the default roles every person gets.

## Developing the JavaScript

To start the yarn debug server and live reload your javascript changes.

```shell
make watch-admin
```

> [!NOTE]
> If you change environment variables, you will have to restart the server.

## Releasing New JavaScript

The admin tool is automatically built as part of our normal CI flow.
