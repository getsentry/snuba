import uuid
from datetime import datetime, timedelta
from typing import Callable, Mapping, Sequence

import pytest

from snuba import optimize, settings, util
from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_writable_storage
from snuba.optimize import (
    JobTimeoutException,
    _get_metrics_tags,
    _subdivide_parts,
    optimize_partition_runner,
)
from snuba.optimize_tracker import RedisOptimizedPartitionTracker
from snuba.processor import InsertBatch
from snuba.redis import redis_client
from snuba.util import Part
from tests.helpers import write_processed_messages

test_data = [
    pytest.param(
        StorageKey.ERRORS,
        lambda dt: InsertBatch(
            [
                {
                    "event_id": str(uuid.uuid4()),
                    "project_id": 1,
                    "group_id": 1,
                    "deleted": 0,
                    "timestamp": dt,
                    "retention_days": settings.DEFAULT_RETENTION_DAYS,
                }
            ],
            None,
        ),
        1,
        id="errors non-parallel",
    ),
    pytest.param(
        StorageKey.ERRORS,
        lambda dt: InsertBatch(
            [
                {
                    "event_id": str(uuid.uuid4()),
                    "project_id": 1,
                    "group_id": 1,
                    "deleted": 0,
                    "timestamp": dt,
                    "retention_days": settings.DEFAULT_RETENTION_DAYS,
                }
            ],
            None,
        ),
        2,
        id="errors parallel",
    ),
    pytest.param(
        StorageKey.TRANSACTIONS,
        lambda dt: InsertBatch(
            [
                {
                    "event_id": str(uuid.uuid4()),
                    "project_id": 1,
                    "deleted": 0,
                    "finish_ts": dt,
                    "retention_days": settings.DEFAULT_RETENTION_DAYS,
                }
            ],
            None,
        ),
        1,
        id="transactions non-parallel",
    ),
    pytest.param(
        StorageKey.TRANSACTIONS,
        lambda dt: InsertBatch(
            [
                {
                    "event_id": str(uuid.uuid4()),
                    "project_id": 1,
                    "deleted": 0,
                    "finish_ts": dt,
                    "retention_days": settings.DEFAULT_RETENTION_DAYS,
                }
            ],
            None,
        ),
        2,
        id="transactions parallel",
    ),
]


class TestOptimize:
    @pytest.mark.parametrize(
        "storage_key, create_event_row_for_date, parallel", test_data,
    )
    def test_optimize(
        self,
        storage_key: StorageKey,
        create_event_row_for_date: Callable[[datetime], InsertBatch],
        parallel: int,
    ) -> None:
        storage = get_writable_storage(storage_key)
        cluster = storage.get_cluster()
        clickhouse = cluster.get_query_connection(ClickhouseClientSettings.OPTIMIZE)
        table = storage.get_table_writer().get_schema().get_local_table_name()
        database = cluster.get_database()

        # no data, 0 partitions to optimize
        parts = optimize.get_partitions_to_optimize(
            clickhouse, storage, database, table
        )
        assert parts == []

        base = datetime(1999, 12, 26)  # a sunday
        base_monday = base - timedelta(days=base.weekday())

        # 1 event, 0 unoptimized parts
        write_processed_messages(storage, [create_event_row_for_date(base)])
        parts = optimize.get_partitions_to_optimize(
            clickhouse, storage, database, table
        )
        assert parts == []

        # 2 events in the same part, 1 unoptimized part
        write_processed_messages(storage, [create_event_row_for_date(base)])
        parts = optimize.get_partitions_to_optimize(
            clickhouse, storage, database, table
        )
        assert [(p.date, p.retention_days) for p in parts] == [(base_monday, 90)]

        # 3 events in the same part, 1 unoptimized part
        write_processed_messages(storage, [create_event_row_for_date(base)])
        parts = optimize.get_partitions_to_optimize(
            clickhouse, storage, database, table
        )
        assert [(p.date, p.retention_days) for p in parts] == [(base_monday, 90)]

        # 3 events in one part, 2 in another, 2 unoptimized parts
        a_month_earlier = base_monday - timedelta(days=31)
        a_month_earlier_monday = a_month_earlier - timedelta(
            days=a_month_earlier.weekday()
        )
        write_processed_messages(
            storage, [create_event_row_for_date(a_month_earlier_monday)]
        )
        write_processed_messages(
            storage, [create_event_row_for_date(a_month_earlier_monday)]
        )
        parts = optimize.get_partitions_to_optimize(
            clickhouse, storage, database, table
        )
        assert [(p.date, p.retention_days) for p in parts] == [
            (base_monday, 90),
            (a_month_earlier_monday, 90),
        ]

        # respects before (base is properly excluded)
        assert [
            (p.date, p.retention_days)
            for p in list(
                optimize.get_partitions_to_optimize(
                    clickhouse, storage, database, table, before=base
                )
            )
        ] == [(a_month_earlier_monday, 90)]

        optimize.optimize_partition_runner(
            clickhouse=clickhouse,
            database=database,
            table=table,
            parts=parts,
            parallel=parallel,
            cutoff_time=None,
            optimize_partition_tracker=None,
        )

        # all parts should be optimized
        parts = optimize.get_partitions_to_optimize(
            clickhouse, storage, database, table
        )
        assert parts == []

    @pytest.mark.parametrize(
        "table,host,expected",
        [
            ("errors_local", None, {"table": "errors_local"},),
            (
                "errors_local",
                "some-hostname.domain.com",
                {"table": "errors_local", "host": "some-hostname.domain.com"},
            ),
        ],
    )
    def test_metrics_tags(
        self, table: str, host: str, expected: Mapping[str, str]
    ) -> None:
        assert _get_metrics_tags(table, host) == expected

    @pytest.mark.parametrize(
        "parts,subdivisions,expected",
        [
            pytest.param(
                [Part("1", datetime(2022, 3, 28), 90)],
                2,
                [[Part("1", datetime(2022, 3, 28), 90)], []],
                id="one part",
            ),
            pytest.param(
                [
                    Part("1", datetime(2022, 3, 28), 90),
                    Part("2", datetime(2022, 3, 21), 90),
                ],
                2,
                [
                    [Part("1", datetime(2022, 3, 28), 90)],
                    [Part("2", datetime(2022, 3, 21), 90)],
                ],
                id="two parts",
            ),
            pytest.param(
                [
                    Part("1", datetime(2022, 3, 28), 90),
                    Part("2", datetime(2022, 3, 21), 90),
                    Part("3", datetime(2022, 3, 28), 30),
                ],
                2,
                [
                    [
                        Part("1", datetime(2022, 3, 28), 90),
                        Part("2", datetime(2022, 3, 21), 90),
                    ],
                    [Part("3", datetime(2022, 3, 28), 30)],
                ],
                id="three parts",
            ),
            pytest.param(
                [
                    Part("1", datetime(2022, 3, 28), 90),
                    Part("2", datetime(2022, 3, 21), 90),
                    Part("3", datetime(2022, 3, 28), 30),
                    Part("4", datetime(2022, 3, 21), 30),
                ],
                2,
                [
                    [
                        Part("1", datetime(2022, 3, 28), 90),
                        Part("2", datetime(2022, 3, 21), 90),
                    ],
                    [
                        Part("3", datetime(2022, 3, 28), 30),
                        Part("4", datetime(2022, 3, 21), 30),
                    ],
                ],
                id="four parts",
            ),
            pytest.param(
                [
                    Part("1", datetime(2022, 3, 28), 90),
                    Part("2", datetime(2022, 3, 21), 90),
                    Part("3", datetime(2022, 3, 28), 30),
                    Part("4", datetime(2022, 3, 21), 30),
                    Part("5", datetime(2022, 3, 14), 90),
                    Part("6", datetime(2022, 3, 7), 90),
                ],
                3,
                [
                    [
                        Part("1", datetime(2022, 3, 28), 90),
                        Part("4", datetime(2022, 3, 21), 30),
                    ],
                    [
                        Part("3", datetime(2022, 3, 28), 30),
                        Part("5", datetime(2022, 3, 14), 90),
                    ],
                    [
                        Part("2", datetime(2022, 3, 21), 90),
                        Part("6", datetime(2022, 3, 7), 90),
                    ],
                ],
                id="six parts",
            ),
        ],
    )
    def test_subdivide_parts(
        self,
        parts: Sequence[util.Part],
        subdivisions: int,
        expected: Sequence[Sequence[util.Part]],
    ) -> None:
        assert _subdivide_parts(parts, subdivisions) == expected


def test_optimize_runner_raises_exception_with_cutoff_time() -> None:
    """
    Tests that a JobTimeoutException is raised when a cutoff time is reached.
    """
    storage = get_writable_storage(StorageKey.ERRORS)
    cluster = storage.get_cluster()
    clickhouse_pool = cluster.get_query_connection(ClickhouseClientSettings.OPTIMIZE)
    table = storage.get_table_writer().get_schema().get_local_table_name()
    database = cluster.get_database()

    parts = [util.Part("1", datetime.now(), 90)]
    with pytest.raises(JobTimeoutException):
        optimize_partition_runner(
            clickhouse=clickhouse_pool,
            database=database,
            table=table,
            parts=parts,
            parallel=2,
            cutoff_time=datetime.now(),
            optimize_partition_tracker=RedisOptimizedPartitionTracker(
                redis_client=redis_client,
                host="some-hostname.domain.com",
                database=database,
                table=table,
                expire_time=datetime.now(),
            ),
        )
