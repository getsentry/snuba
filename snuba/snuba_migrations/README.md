# Migration groups

Each directory here is a migration group. Loaders in `snuba/migrations/group_loader.py` map groups to directories.

Name each migration `xxxx_migration_name.py`, where `xxxx` is the zero-padded migration number, for example `events/0001_events_initial.py`. Migrations in a group run in number order.

To run, write, or generate migrations, see [the migrations guide](../../docs/source/migrations/overview.md).
