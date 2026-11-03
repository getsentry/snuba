from collections.abc import Generator

import psycopg
import pytest
from sqlalchemy import Engine, inspect

from snuba import settings
from snuba.subscriptions.postgres.database import (
    build_engine,
    create_database_if_missing,
    migrate_to_head,
)


@pytest.fixture(scope="module")
def engine() -> Generator[Engine]:
    url = settings.SUBSCRIPTIONS_POSTGRES_URL
    assert url is not None
    create_database_if_missing(url)
    migrate_to_head()
    engine = build_engine(url)
    yield engine
    engine.dispose()


def test_subscriptions_table_schema(engine: Engine) -> None:
    inspector = inspect(engine)
    columns = inspector.get_columns("subscriptions")
    shapes = {column["name"]: (str(column["type"]), column["nullable"]) for column in columns}
    assert shapes == {
        "id": ("UUID", False),
        "entity": ("TEXT", False),
        "partition": ("INTEGER", False),
        "data": ("JSONB", False),
    }

    primary_key = inspector.get_pk_constraint("subscriptions")
    assert primary_key["constrained_columns"] == ["id"]

    indexes = inspector.get_indexes("subscriptions")
    index_columns = {index["name"]: index["column_names"] for index in indexes}
    assert index_columns == {"subscriptions_entity_partition_idx": ["entity", "partition"]}


def test_engine_disables_prepared_statements(engine: Engine) -> None:
    with engine.connect() as connection:
        dbapi_connection = connection.connection.dbapi_connection
        assert isinstance(dbapi_connection, psycopg.Connection)
        assert dbapi_connection.prepare_threshold is None
