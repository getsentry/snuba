If you don't know anything about snuba migrations see `MIGRATIONS.md` in the root folder

Each folder in here represents a migration group. see `snuba/migrations/group_loader.py`

Each migration (ex. `events/0001_events_initial.py`) needs to follow the naming scheme `xxxx_migration_name.py`
where `xxxx` is the 0 padded migration number. Migrations are applied in order of migration number. See `snuba/migrations/group_loader.py` for more info.

## Migration Auto-Generation
Who wants to write their own migrations by hand? Certainly not me! See `MIGRATIONS.md` to learn how you can have them generated for you.
