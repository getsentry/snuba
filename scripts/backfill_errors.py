"""\
Backfills the errors table from events.

This script will eventually be moved to a migration - after we have a multistorage
consumer running in all environments populating new events into both tables.

Errors replacements should be turned off while this script is running.
"""
from snuba.migrations.backfill_errors import backfill_errors

backfill_errors()
