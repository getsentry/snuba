"""
Asynchronous execution of ``ON CLUSTER`` DDL for migrations.

Running ``ON CLUSTER`` DDL synchronously (the ClickHouse default,
``distributed_ddl_output_mode=throw``) has a bad failure mode: the server streams
the per-host result table as hosts report in, and if a host does not report
within ``distributed_ddl_task_timeout`` it raises ``TIMEOUT_EXCEEDED`` *in the
middle of the response body*. Because the headers and the first rows are already
on the wire, the server cannot emit a well-formed error payload -- it just closes
the connection. The client observes::

    Connection broken: IncompleteRead(676 bytes read)

which says nothing about which replica failed or why.

Instead we submit the DDL with ``distributed_ddl_task_timeout=0`` (fire and
forget, returns as soon as the task is queued in Keeper) and then poll
``system.distributed_ddl_queue``, which reports ``status``, ``exception_code``
and ``exception_text`` per host. The task is correlated via a unique
``log_comment`` set on the submitting query, which ClickHouse persists in the
queue entry's ``settings`` map.

The DDL itself is unaffected: it is coordinated through Keeper exactly as before.
Only the way we *observe* its completion changes.
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Mapping
from typing import Any

import structlog

from snuba import settings
from snuba.clickhouse.escaping import escape_string
from snuba.clickhouse.native import ClickhousePool

logger = structlog.get_logger().bind(module=__name__)

# The terminal state in system.distributed_ddl_queue's status enum
# (Inactive / Active / Finished / Removing / Unknown). Any other value means the
# host has not applied the DDL yet -- 'Inactive' in particular is a replica that
# is down or has not noticed the queue entry.
_FINISHED = "Finished"


class DistributedDDLTimeout(Exception):
    """An async ON CLUSTER DDL task did not finish on every host in time."""


class DistributedDDLError(Exception):
    """An async ON CLUSTER DDL task failed on at least one host."""


def execute_ddl_async(
    connection: ClickhousePool,
    sql: str,
    cluster_name: str,
    query_settings: Mapping[str, Any] | None = None,
    timeout_seconds: int | None = None,
    poll_interval_seconds: float | None = None,
) -> None:
    """
    Submit ``sql`` (which must already contain an ``ON CLUSTER`` clause) without
    waiting for the cluster, then poll until every host reports ``Finished``.

    Raises :class:`DistributedDDLError` if any host reported a non-zero
    ``exception_code``, or :class:`DistributedDDLTimeout` if some host had not
    finished within the timeout. In the timeout case the DDL task remains queued
    in Keeper and will still be applied when the lagging replica recovers, so the
    migration can simply be re-run (migration DDL is written to be idempotent).
    """
    if timeout_seconds is None:
        timeout_seconds = settings.ASYNC_MIGRATION_DDL_TIMEOUT_SECONDS
    if poll_interval_seconds is None:
        poll_interval_seconds = settings.ASYNC_MIGRATION_DDL_POLL_INTERVAL_SECONDS

    # Correlates the queue entry back to this specific submission. Matching on the
    # query text is not reliable: ClickHouse rewrites it before storing (it
    # normalizes the table name and injects a UUID), and identical DDL may have
    # been submitted before.
    task_id = f"snuba-migration-{uuid.uuid4()}"

    submit_settings: dict[str, Any] = dict(query_settings or {})
    # 0 == "do not wait for any host", return once the task is in Keeper.
    submit_settings["distributed_ddl_task_timeout"] = 0
    # With task_timeout=0 nothing is awaited, so no truncated-body failure is
    # possible; never_throw additionally keeps a slow/unavailable replica from
    # turning the submission itself into an error.
    submit_settings["distributed_ddl_output_mode"] = "never_throw"
    submit_settings["log_comment"] = task_id

    logger.info("Submitting async ON CLUSTER DDL", task_id=task_id, cluster=cluster_name)
    connection.execute(sql, settings=submit_settings)

    _wait_for_ddl_task(
        connection,
        task_id=task_id,
        cluster_name=cluster_name,
        timeout_seconds=timeout_seconds,
        poll_interval_seconds=poll_interval_seconds,
    )


def _wait_for_ddl_task(
    connection: ClickhousePool,
    task_id: str,
    cluster_name: str,
    timeout_seconds: int,
    poll_interval_seconds: float,
) -> None:
    query = f"""
        SELECT
            entry,
            host,
            status,
            exception_code,
            exception_text
        FROM system.distributed_ddl_queue
        WHERE settings['log_comment'] = {escape_string(task_id)}
    """

    deadline = time.monotonic() + timeout_seconds
    last_seen: list[tuple[str, str, int, str]] = []

    while True:
        rows = connection.execute(query).results

        # Rows may be briefly absent: the initiator returns as soon as the task is
        # written to Keeper, and the queue table is populated from it. Treat an
        # empty result as "not visible yet" and keep polling until the deadline.
        if rows:
            statuses: list[tuple[str, str, int, str]] = [
                (
                    str(host),
                    str(status),
                    int(exception_code or 0),
                    str(exception_text or ""),
                )
                for _entry, host, status, exception_code, exception_text in rows
            ]
            last_seen = statuses

            failures = [(host, code, text) for host, _status, code, text in statuses if code != 0]
            if failures:
                detail = "; ".join(f"{host}: [{code}] {text}" for host, code, text in failures)
                raise DistributedDDLError(
                    f"ON CLUSTER DDL failed on {len(failures)} host(s) of "
                    f"cluster '{cluster_name}' (task {task_id}): {detail}"
                )

            pending = [host for host, status, _c, _t in statuses if status != _FINISHED]
            if not pending:
                logger.info(
                    "Async ON CLUSTER DDL finished on all hosts",
                    task_id=task_id,
                    hosts=len(statuses),
                )
                return

        if time.monotonic() >= deadline:
            pending = [
                f"{host} ({status})" for host, status, _c, _t in last_seen if status != _FINISHED
            ]
            unfinished = ", ".join(pending) if pending else "task not visible in queue"
            raise DistributedDDLTimeout(
                f"ON CLUSTER DDL on cluster '{cluster_name}' (task {task_id}) did not "
                f"complete within {timeout_seconds}s. Unfinished hosts: {unfinished}. "
                "The task is still queued in ClickHouse Keeper and will be applied "
                "once those replicas recover; check `SELECT * FROM "
                "system.distributed_ddl_queue WHERE settings['log_comment'] = "
                f"'{task_id}'` and `system.replicas`, then re-run the migration."
            )

        time.sleep(poll_interval_seconds)


__all__ = [
    "DistributedDDLError",
    "DistributedDDLTimeout",
    "execute_ddl_async",
]
