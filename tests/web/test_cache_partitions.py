from __future__ import annotations

from typing import Any

import pytest

from snuba import state
from snuba.attribution.appid import AppID
from snuba.attribution.attribution_info import AttributionInfo
from snuba.clickhouse.formatter.query import format_query
from snuba.clickhouse.native import ClickhousePool, NativeDriverReader
from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query import SelectedExpression
from snuba.query.data_source.simple import Table
from snuba.query.parser.expressions import parse_clickhouse_function
from snuba.query.query_settings import HTTPQuerySettings
from snuba.utils.metrics.timer import Timer
from snuba.web.db_query_class import DBQuery


def _build_test_query() -> tuple[ClickhouseQuery, AttributionInfo]:
    storage = get_storage(StorageKey("errors_ro"))
    return (
        ClickhouseQuery(
            from_clause=Table(
                storage.get_schema().get_data_source().get_table_name(),  # type: ignore
                schema=storage.get_schema().get_columns(),
                final=False,
                allocation_policies=storage.get_allocation_policies(),
            ),
            selected_columns=[
                SelectedExpression(
                    "some_alias",
                    parse_clickhouse_function("lol"),
                )
            ],
        ),
        AttributionInfo(
            app_id=AppID(key="key"),
            tenant_ids={"referrer": "something", "organization_id": 1234},
            referrer="something",
            team=None,
            feature=None,
            parent_api=None,
        ),
    )


def __build_db_query_obj(
    reader: NativeDriverReader, clickhouse_query_settings: dict[str, Any] | None = None
) -> DBQuery:
    query, attribution_info = _build_test_query()
    obj = DBQuery(
        clickhouse_query=query,
        query_settings=HTTPQuerySettings(),
        attribution_info=attribution_info,
        dataset_name="",
        query_metadata_list=[],
        formatted_query=format_query(query),
        reader=reader,
        timer=Timer("foo"),
        stats={},
    )
    if clickhouse_query_settings:
        obj.clickhouse_query_settings = clickhouse_query_settings
    return obj


@pytest.mark.redis_db
def test_cache_partition() -> None:
    pool = ClickhousePool("127.0.0.1", 9000, "", "", "")
    reader1 = NativeDriverReader(None, pool, None)
    reader2 = NativeDriverReader(None, pool, None)

    default_cache = __build_db_query_obj(reader1)._get_cache_partition()
    another_default_cache = __build_db_query_obj(reader2)._get_cache_partition()

    assert id(default_cache) == id(another_default_cache)

    reader3 = NativeDriverReader("non_default", pool, None)
    reader4 = NativeDriverReader("non_default", pool, None)
    nondefault_cache = __build_db_query_obj(reader3)._get_cache_partition()
    another_nondefault_cache = __build_db_query_obj(reader4)._get_cache_partition()

    assert id(nondefault_cache) == id(another_nondefault_cache)
    assert id(default_cache) != id(nondefault_cache)


@pytest.mark.redis_db
def test_cache_wait_timeout() -> None:
    pool = ClickhousePool("127.0.0.1", 9000, "", "", "")
    default_reader = NativeDriverReader(None, pool, None)
    tiger_errors_reader = NativeDriverReader("tiger_errors", pool, None)
    tiger_transactions_reader = NativeDriverReader("tiger_transactions", pool, None)

    query_settings = {"max_execution_time": 30}
    assert (
        __build_db_query_obj(default_reader, query_settings)._get_cache_wait_timeout()
        == 30
    )
    assert (
        __build_db_query_obj(
            tiger_errors_reader, query_settings
        )._get_cache_wait_timeout()
        == 30
    )
    assert (
        __build_db_query_obj(
            tiger_transactions_reader, query_settings
        )._get_cache_wait_timeout()
        == 30
    )

    state.set_config("tiger-cache-wait-time", 60)
    assert (
        __build_db_query_obj(default_reader, query_settings)._get_cache_wait_timeout()
        == 30
    )
    assert (
        __build_db_query_obj(
            tiger_errors_reader, query_settings
        )._get_cache_wait_timeout()
        == 60
    )
    assert (
        __build_db_query_obj(
            tiger_transactions_reader, query_settings
        )._get_cache_wait_timeout()
        == 60
    )

    state.delete_config("tiger-cache-wait-time")
