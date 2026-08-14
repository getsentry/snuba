from __future__ import annotations

from collections.abc import Mapping
from typing import Any, cast

from snuba.clickhouse.formatter.nodes import FormattedQuery
from snuba.clickhouse.pool import ClickhousePool, ClickhouseResult
from snuba.reader import Reader, Result


class ClickhouseReader(Reader):
    """Adapt a ClickhouseResult into the Result used by query paths."""

    def __init__(
        self,
        cache_partition_id: str | None,
        client: ClickhousePool,
        query_settings_prefix: str | None,
    ) -> None:
        super().__init__(
            cache_partition_id=cache_partition_id,
            query_settings_prefix=query_settings_prefix,
        )
        self.__client = client

    def __transform_result(self, result: ClickhouseResult, with_totals: bool) -> Result:
        meta = result.meta if result.meta is not None else []
        data = result.results
        profile = cast(dict[str, Any] | None, result.profile)
        # Rows are mappings keyed by column/alias. Duplicate names are discarded
        # so headers and row data stay consistent.
        columns = {c[0]: i for i, c in enumerate(meta)}

        if not isinstance(data, list):
            data = list(data)
        column_items = list(columns.items())
        for i, row in enumerate(data):
            data[i] = {column: row[index] for column, index in column_items}

        meta = [{"name": m[0], "type": m[1]} for m in [meta[i] for i in columns.values()]]

        if with_totals:
            assert len(data) > 0, "WITH TOTALS query returned no rows (missing totals row)"
            totals = data.pop(-1)
            return {
                "data": data,
                "meta": meta,
                "totals": totals,
                "profile": profile,
                "trace_output": result.trace_output,
            }
        return {
            "data": data,
            "meta": meta,
            "profile": profile,
            "trace_output": result.trace_output,
        }

    def execute(
        self,
        query: FormattedQuery,
        settings: Mapping[str, str] | None = None,
        with_totals: bool = False,
        robust: bool = False,
        capture_trace: bool = False,
    ) -> Result:
        settings = {**settings} if settings is not None else {}

        query_id = None
        if "query_id" in settings:
            query_id = settings.pop("query_id")

        if with_totals:
            result = self.__client.execute_with_totals(
                query.get_sql(),
                query_id=query_id,
                settings=settings,
                capture_trace=capture_trace,
                robust=robust,
            )
        else:
            execute_func = self.__client.execute_robust if robust else self.__client.execute
            result = execute_func(
                query.get_sql(),
                with_column_types=True,
                query_id=query_id,
                settings=settings,
                capture_trace=capture_trace,
            )

        return self.__transform_result(result, with_totals=with_totals)
