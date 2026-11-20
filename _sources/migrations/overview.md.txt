# Snuba migrations

Migrations define and apply ClickHouse schema changes as the Snuba codebase changes. You manage them with the `snuba migrations` CLI.

## Types of migrations

1. ClickHouse node migrations (`ClickhouseNodeMigration`) run SQL statements on one or many ClickHouse nodes. Most migrations change the schema, such as adding or altering tables, and use this type.
2. Code migrations (`CodeMigration`) run Python functions. Use them for data migrations or custom behavior that SQL cannot express. They are less common.

## Migration groups

Migrations are organized into groups that usually correspond to a feature or a set of related tables. Groups are ordered, and every migration in a group runs before any migration in the next group. A migration can depend on changes from earlier groups, but not from later ones. The `system` group creates the table the migration system itself uses, and it must stay first.

Each group is a directory in `snuba/snuba_migrations/`, except `system`, which lives in `snuba/migrations/system_migrations/`. The groups and their order are registered in `_REGISTERED_MIGRATION_GROUPS` in `snuba/migrations/groups.py`.

Groups are mandatory by default. Groups listed in `OPTIONAL_GROUPS` in `snuba/migrations/groups.py` can be skipped with `settings.SKIPPED_MIGRATION_GROUPS`. In most cases, keep the default in `snuba/settings/__init__.py`.

## Configure settings.CLUSTERS

`settings.CLUSTERS` maps storage sets (groups of tables that must be colocated) to ClickHouse clusters. Every storage set must be assigned to one cluster.

For each cluster, set `single_node` to `True` or `False`. If it is `False`, also set `cluster_name` and `distributed_cluster_name` to the ClickHouse cluster names in `system.clusters` where the data should be stored. For a single node cluster, keep the default configuration. For distributed mode, see {doc}`modes`.

## Run migrations

### List migrations

`snuba migrations list` lists all migrations and their statuses.

### Run all migrations

`snuba migrations migrate --force` runs all pending migrations and brings the database to the latest state.

- `--force` also runs migrations marked blocking, which usually contain a data migration. Blocking migrations can take a long time. Stop any consumers that write to the affected table before you run them.
- Without `--force`, the command runs only if no pending migration is blocking.
- `snuba devserver` runs this command on startup, so you do not need to run it yourself.

### Run a single migration

`snuba migrations run --group <group> --migration-id <migration_id>`

- The command is allowed only if every earlier migration in that group is completed.
- `--dry-run` prints the migration's SQL to stdout. It does not work for code migrations.

### Reverse a single migration

`snuba migrations reverse --group <group> --migration-id <migration_id>`

- The command is allowed only if no later migration in that group is completed.
- If the migration is already marked completed, add `--force`.
- `--dry-run` prints the migration's SQL to stdout. It does not work for code migrations.

### Fake migrations

`snuba migrations migrate --fake` marks every migration as completed in the `migrations_local` table without running it.

- If you fake a blocking migration, add `--force`.
- To fake one migration, run `snuba migrations run` with `--fake`. Only that migration's status changes.

## Write migrations

### Generate a migration

`snuba migrations generate` writes a migration for you. It supports only adding a column to an existing table.

1. Find the storage file for the table, at `snuba/datasets/configuration/<dataset>/storages/<storage>.yaml`. For example, `snuba/datasets/configuration/group_attributes/storages/group_attributes.yaml`.
2. Edit the file. To add a column, add it to `schema.columns`. Leave the change uncommitted, because the command diffs the working tree against `HEAD`.
3. From the repository root, run `snuba migrations generate path/to/storage.yaml`. The command writes the migration to `snuba/snuba_migrations/<storage set>/`.
4. Commit the storage change and the generated migration together.

### Add a migration by hand

1. Pick the group for the migration.
2. Add a file at `snuba/snuba_migrations/<group>/<migration_id>.py`. Prefix the migration ID with its zero-padded position in the group. For example, the fourth migration in a group starts with `0004_`.
3. In the file, define a class named `Migration` that inherits from `ClickhouseNodeMigration` or `CodeMigration`.

To create a new group:

1. Add the group to `MigrationGroup` in `snuba/migrations/groups.py`.
2. Add a loader for the group to `snuba/migrations/group_loader.py`. The loader names the directory that holds the group's migrations.
3. Register the group in `_REGISTERED_MIGRATION_GROUPS`. Its position in that dict sets the order in which groups run.
4. Set `readiness_state` to `ReadinessState.LIMITED`, so the migrations run automatically only in CI and local environments.

### Add an SQL migration (ClickhouseNodeMigration)

Define `forwards_ops` and `backwards_ops`. Each returns a sequence of SQL operations for the forwards and backwards (reverse) migration, covering every ClickHouse layout a user may have.

Each operation takes a `target` argument that says whether it applies to the local or the distributed table: `OperationTarget.LOCAL` or `OperationTarget.DISTRIBUTED`. The default is `OperationTarget.UNSET`, and the runner raises an error for an operation with an unset target.

Apply `AddColumn` and `CreateTable` to local tables before distributed tables. Apply `DropColumn` to distributed tables first. Otherwise, the distributed table can briefly reference a column that does not exist. For example, to add `col1` to a local and a distributed table, `forwards_ops` returns:

```python
[
    AddColumn(target=OperationTarget.LOCAL, ..., Column("col1")),
    AddColumn(target=OperationTarget.DISTRIBUTED, ..., Column("col1")),
]
```

`validate_migration_order` in `snuba/migrations/validator.py` checks for these ordering conflicts. `tests/migrations/test_validator.py` runs it for all migrations.

If the forwards operations fail halfway, the backwards operations must restore the original state so you can retry the migration. For example, if the forwards migration creates a temporary table, the backwards migration drops it.

After you add the migration, update the storage schema to match. A test fails if a column in the schema is missing from the database or differs from it after migrating.

### Add a Python migration (CodeMigration)

Define `forwards_global` and `backwards_global`.

### Mark a migration blocking

Mark a migration `blocking` if it cannot complete immediately, for example because it rewrites data. Run a blocking migration with the relevant consumer stopped, or use another strategy, such as dual writing, to avoid downtime while it runs.
