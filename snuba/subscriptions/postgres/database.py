from pathlib import Path
from typing import Any

from alembic import command
from alembic.config import Config
from sqlalchemy import Engine, create_engine, make_url, text

ALEMBIC_INI = Path(__file__).parent / "alembic.ini"

CONNECT_ARGS: dict[str, Any] = {
    # pgbouncer runs the app role in transaction mode, which breaks prepared statements.
    "prepare_threshold": None,
    "connect_timeout": 5,
    # Detect a dead connection in under a minute instead of the OS default of 2 hours.
    "keepalives_idle": 10,
    "tcp_user_timeout": 30000,
}


def build_engine(url: str) -> Engine:
    return create_engine(url, pool_pre_ping=True, connect_args=CONNECT_ARGS)


def create_database_if_missing(url: str) -> None:
    """Create the URL's database if absent; dev and test only, prod's comes from alloydb-init."""
    target = make_url(url)
    maintenance_url = target.set(database="postgres")
    engine = create_engine(maintenance_url, isolation_level="AUTOCOMMIT")
    query = text("SELECT 1 FROM pg_database WHERE datname = :name")
    with engine.connect() as connection:
        result = connection.execute(query, {"name": target.database})
        exists = result.scalar()
        if exists is None:
            create = text(f'CREATE DATABASE "{target.database}"')
            connection.execute(create)
    engine.dispose()


def migrate_to_head() -> None:
    config = Config(str(ALEMBIC_INI))
    command.upgrade(config, "head")
