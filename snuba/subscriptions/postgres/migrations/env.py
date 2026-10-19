from alembic import context

from snuba import settings
from snuba.subscriptions.postgres.database import build_engine

if context.is_offline_mode():
    raise RuntimeError("Offline migrations are not supported")

url = settings.SUBSCRIPTIONS_POSTGRES_MIGRATIONS_URL
if url is None:
    raise RuntimeError("SUBSCRIPTIONS_POSTGRES_MIGRATIONS_URL is not set")

engine = build_engine(url)
with engine.connect() as connection:
    context.configure(connection=connection, target_metadata=None)
    with context.begin_transaction():
        context.run_migrations()
engine.dispose()
