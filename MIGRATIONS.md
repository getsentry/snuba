# Snuba migrations field guide

Snuba provides a set of tools to help manage ClickHouse database schemas and state.

:warning: The migration system in Snuba is still under development and is subject to change. In particular, automated migrations for clustered deployments of ClickHouse (distributed and replicated tables) are not currently supported, but may still be discussed here and partially supported in some parts of the system.

### Quick links:

- [Overview](#overview)

- [Getting started](#getting-started)

- [Running migrations](#running-migrations)

- [Writing migrations](#writing-migrations)

## Overview

Migrations are the mechanism through which database changes are defined and applied so that ClickHouse schemas can be evolved as the Snuba codebase changes.

### Types of migrations

There are two types of migrations with distinct functionality:

1. ClickHouse node migrations. These execute SQL statements on one or many ClickHouse machines. These are typically used to change the ClickHouse schema in some way, such as adding or altering tables. Most migrations fall into this category.

2. Global migrations. A more flexible type of migration that can execute any provided Python functions. Used for data migrations or more custom behavior that cannot be expressed as pure SQL. These are less common.

### Migration groups

Migrations follow a strict sequence to ensure that changes are applied in order, and any dependencies between migrations are met.

Migrations in Snuba are organized into groups, which typically correspond with features or sets of related tables in Snuba. Each migration is applied in order by group, and the groups are themselves ordered. All of the migrations of the first group are applied first, then all of the migrations of the second group, and so on. Hence migrations in subsequent groups can depend on changes in earlier groups being already applied but not vice versa. The `system` group is the one that creates the ClickHouse table used by the migrations system itself, and must always remain as the first group.

In this codebase, each group is represented by a folder in the `snuba.snuba_migrations` module, and all of the migrations for that group are stored within that folder.

Migration groups can be defined as either optional or mandatory. Most groups in Snuba are mandatory, however optional groups can provide a mechanism to test experimental features in Snuba that should not be rolled out to most users yet. Optional groups can be toggled by the user via `settings.SKIPPED_MIGRATION_GROUPS`. In most cases, the default in settings.py should not be changed, and doing so may result in unexpected behavior.

## Getting started

Snuba supports running ClickHouse in either a single node or multi node configuration. Currently only the single node configuration is fully supported through the migration system.

### Configure settings.CLUSTERS

The mapping in `settings.CLUSTERS` defines the relationship between storage sets (groups of tables that must be colocated), and ClickHouse clusters. Every storage set must be assigned to one cluster.

For each cluster, `single_node` can be set to either True or False. If False, `cluster_name` and `distributed_cluster_name` should also be provided. These should align to the ClickHouse cluster names in `system.clusters` where you want the data to be stored.

For a single node cluster, do not change the default configuration.

## Running migrations

The `snuba migrations` CLI tool should be used to manage migrations.

### List migrations

`snuba migrations list`

- Lists all migrations and their statuses

### Run all migrations

`snuba migrations migrate --force`

- Runs all pending migrations and brings your database to the latest state.
- Running with the --force flag means that any migrations marked blocking (generally because they contain a data migration) will also be executed. Blocking migrations may take some time to complete. Running with --force assumes that any consumers filling the corresponding table are stopped and no new data is being written to the table as the migration is taking place.
- Running this without the --force flag, will only execute the migrations if none are blocking.
- If you are running `snuba devserver`, this command automatically run when the devserver is started and there is no need to manage migrations manually.

### Run a single migration

`snuba migrations run --group <group> --migration-id <migration_id>`

- Runs a single migration
- Only allowed if there are no prior migrations in that group that have not been completed
- The `--dry-run` option can be used to simply print migration's SQL to stdout (note: not for Python migrations)

### Reverse a single migration

`snuba migrations reverse --group <group> --migration-id <migration_id>`

- Reverses a single migration
- Only allowed if there are no subsequent completed migrations in that group
- Force must be used if the migration is already marked completed
- The `--dry-run` option can be used to simply print migration's SQL to stdout (note: not for Python migrations)

### Adding a new node

`snuba migrations add-node --type <type> --storage-set <storage_sets>`

- Runs all ClickHouse migrations on a new node so it can be added to an existing ClickHouse cluster
- Since adding a node without any tables on it to an existing cluster will cause all queries to fail we need to run the migrations first
- The type and storage sets should correspond to the cluster where that node will eventually be added
- If non default username/password is required, the credentials for the ClickHouse node should be passed via environment variables
- For obvious reasons, this command is only relevant for multi node clusters

## Writing migrations

In order to add a new migration, first determine which migration group the new migration should be added to, and add an entry to that group in `migrations/groups.py` with the new migration identifier you have chosen. By convention we prefix migration IDs with a number matching the position of the migration in the group, i.e. the 4th migration in that group will be prefixed with `0004_`. Add a file which will contain the new migration at `/migrations/snuba_migrations/<group>/<migration_id>.py`.

If you need to create a new group, add the group to `migrations.groups.MigrationGroup` and a loader to `migrations.group_loader` for the group defining the path to the directory where that group's migrations will be located. Register these to `migrations.groups._REGISTERED_MIGRATION_GROUPS` - note the position of the group in this list determines the order the migrations will be executed in. For a new MigrationGroup, the `readiness_state` should be set to `limited` which means the migrations will only be automatically executed in CI and the local environment.

The new migration should contain a class called `Migration` which inherits from one of two types:
`ClickhouseNodeMigration` or `CodeMigration`

### Adding an SQL migration (ClickhouseNodeMigration)

A ClickhouseNodeMigration is the most common type of migration and determines the SQL to be run on each ClickHouse node. You must define two methods - `forwards_ops` and `backwards_ops` in order to provide the DDL for all ClickHouse layouts that a user may have. These functions return a sequence of sql ops for the forwards and backwards (reverse) migrations.

Each operation in the sequence takes in a `target` parameter in its `__init__` to define if the operation is to be applied on a local or distributed table. `OperationTarget.LOCAL` for local tables, `OperationTarget.DISTRIBUTED` for distributed. By default, `target=OperationTarget.UNSET`. Your migration will not be applied if it is left unset and the runner will throw an error.

For some SQL operations, such as `AddColumn` and `CreateTable`, it's important to apply them on local nodes before distributed. For others, such as `DropColumn`, it's important to apply them on the distributed first. For example, if your migration involves adding a column `col1` to a local and distributed table, your `forward_ops` should return a sequence:
```
[
    AddColumn(target=OperationTarget.LOCAL ...., Column('col1') ),
    AddColumn(target=OperationTarget.DISTRIBUTED ...., Column('col1') )
]
```

Doing operations in the wrong order would risk the distributed table momentarily referencing a non-existent column. To assist in preventing these ordering issues, there is a function `validate_migration_order` in the `snuba.migrations.validator` that checks for ordering conflicts. By default, this function is called for all migrations in `tests/migrations/test_validator.py`

For each forwards method, you should provide the sequence of operations to be run on that node. In case the forwards methods fail halfway, the corresponding backwards methods should also restore the original state so the migration can be retried.
For example if a temporary table is created during the forwards migration, the backwards migration should drop it.

Once the migration has been added, the Snuba schema for that storage should also be updated accordingly. A test will fail if any columns in the schema are missing or inconsistent with what is found in the database post migration.

### Adding a Python migration (CodeMigration)

A CodeMigration can be used if the migration requires more complex behavior and needs to run custom Python code. You should provide the `forwards_global` and `backwards_global` methods.

### The `blocking` flag

A migration that can not immediately complete (i.e. requires data to be rewritten) should be marked as `blocking`. Typically this indicates that the migration needs to be run with the relevant consumer stopped or an alternate strategy (e.g. dual writing employed to ensure no downtime as the migration is running.
