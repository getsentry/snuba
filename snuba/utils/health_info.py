from __future__ import annotations

import logging
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, TimeoutError, as_completed
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, MutableMapping, Set, Union, cast

import sentry_sdk
import simplejson as json

from snuba import environment, settings
from snuba.clickhouse.errors import ClickhouseError
from snuba.clusters.cluster import (
    ClickhouseClientSettings,
    ClickhouseCluster,
    ConnectionId,
    UndefinedClickhouseCluster,
)
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.schemas.tables import TableSchema
from snuba.datasets.storage import Storage
from snuba.datasets.storages.factory import get_all_storage_keys, get_storage
from snuba.environment import setup_logging
from snuba.state import get_float_config, get_int_config
from snuba.utils.metrics.wrapper import MetricsWrapper

metrics = MetricsWrapper(environment.metrics, "api")
setup_logging(None)
logger = logging.getLogger("snuba.health")


# INC-2141: clusters whose connectivity is required for Snuba to be considered
# healthy. Identified by storage_set_key, which maps 1:1 to a cluster in
# settings.CLUSTERS. Failing any of these will fail the health check so the
# pod can be recycled promptly.
ESSENTIAL_STORAGE_SET_KEYS = {
    StorageSetKey.EVENTS,  # errors storage
    StorageSetKey.EVENTS_RO,  # errors_ro storage
    StorageSetKey.EVENTS_ANALYTICS_PLATFORM,
}


def _execute_show_tables(cluster: ClickhouseCluster) -> bool:
    clickhouse = cluster.get_query_connection(ClickhouseClientSettings.QUERY)
    clickhouse.execute("show tables").results
    return True


@dataclass
class HealthInfo:
    body: str
    status: int
    content_type: Dict[str, str]


def get_health_info(thorough: Union[bool, str]) -> HealthInfo:
    start = time.time()
    metric_tags = {"thorough": str(thorough)}

    if get_int_config("health_check_ignore_clickhouse", 0) == 1:
        clickhouse_health = True
        metric_tags["skipped_clickhouse_check"] = "true"
    else:
        clickhouse_health = (
            check_all_tables_present(metric_tags=metric_tags)
            if thorough
            else sanity_check_clickhouse_connections()
        )

    metric_tags["clickhouse_ok"] = str(clickhouse_health)

    body: Mapping[str, Union[str, bool]]
    if clickhouse_health:
        body = {"status": "ok"}
        status = 200
    else:
        body = {}
        if thorough:
            body["clickhouse_ok"] = clickhouse_health
        status = 502

    payload = json.dumps(body)
    if status != 200:
        metrics.increment("healthcheck_failed", tags=metric_tags)
        logger.info("Snuba health check failed! Tags: %s", metric_tags)

    metrics.timing(
        "healthcheck.latency",
        time.time() - start,
        tags={"thorough": str(thorough)},
    )

    return HealthInfo(
        body=payload,
        status=status,
        content_type={"Content-Type": "application/json"},
    )


def filter_checked_storages(filtered_storages: List[Storage]) -> None:
    all_storage_keys = get_all_storage_keys()
    for storage_key in all_storage_keys:
        storage = get_storage(storage_key)
        if storage.get_readiness_state().value in settings.SUPPORTED_STATES:
            filtered_storages.append(storage)


def sanity_check_clickhouse_connections(timeout_seconds: float = 0.5) -> bool:
    """
    Check that every essential ClickHouse cluster (see ESSENTIAL_STORAGE_SET_KEYS)
    is operable. Returns True only if all essential clusters respond
    successfully within the timeout, False otherwise.

    Per INC-2141: previously this short-circuited to True if *any* cluster
    responded, which delayed pod recycling when a high-traffic cluster (EAP)
    held stalled connections while other clusters answered fine. We now fail
    health if any essential cluster is unreachable.

    Every individual query node check is limited to a `timeout_seconds` timeout
    (default 0.5 or 500ms).
    """
    timeout_seconds = get_float_config("health_check.timeout_override_seconds", timeout_seconds)  # type: ignore

    storages: List[Storage] = []
    try:
        filter_checked_storages(storages)
    except KeyError:
        pass

    unique_clusters: dict[ConnectionId, ClickhouseCluster] = {}
    for storage in storages:
        if storage.get_storage_set_key() not in ESSENTIAL_STORAGE_SET_KEYS:
            continue
        try:
            cluster = storage.get_cluster()
            unique_clusters[cluster.get_connection_id()] = cluster
        except UndefinedClickhouseCluster as err:
            logger.error(err)
            return False

    if not unique_clusters:
        logger.error("No essential ClickHouse clusters found for health check")
        return False

    # Don't use `with ThreadPoolExecutor(...)` — its __exit__ calls
    # shutdown(wait=True), which blocks on stalled futures and defeats the
    # timeout (the exact INC-2141 scenario). Instead, shutdown(wait=False) on
    # the way out so a stalled connection can't keep the health check pinned.
    # Stalled worker threads will run to completion in the background; that's
    # acceptable because the pod is failing health and will be recycled.
    executor = ThreadPoolExecutor(
        max_workers=len(unique_clusters), thread_name_prefix="health-check"
    )
    try:
        future_to_cluster = {
            executor.submit(_execute_show_tables, cluster): cluster
            for cluster in unique_clusters.values()
        }

        healthy = True
        completed: Set[ConnectionId] = set()
        try:
            for future in as_completed(future_to_cluster, timeout=timeout_seconds):
                cluster = future_to_cluster[future]
                completed.add(cluster.get_connection_id())
                try:
                    future.result()
                except Exception as err:
                    with sentry_sdk.new_scope() as scope:
                        scope.set_tag("health_cluster_name", cluster.get_clickhouse_cluster_name())
                        logger.error(err)
                    healthy = False
        except TimeoutError:
            unfinished = [
                cluster.get_clickhouse_cluster_name()
                for connection_id, cluster in unique_clusters.items()
                if connection_id not in completed
            ]
            logger.info(
                f"Essential ClickHouse clusters did not respond within {timeout_seconds}s: {unfinished}"
            )
            healthy = False
    finally:
        executor.shutdown(wait=False)

    return healthy


def check_all_tables_present(metric_tags: dict[str, Any] | None = None) -> bool:
    """
    Checks if all the tables in all the enabled datasets exist in ClickHouse
    TODO: Eventually, when we fully migrate to readiness_states, we can remove DISABLED_DATASETS.
    """

    setup_logging(None)
    logger = logging.getLogger("snuba.health")

    try:
        storages: List[Storage] = []
        filter_checked_storages(storages)
        connection_grouped_table_names: MutableMapping[ConnectionId, Set[str]] = defaultdict(set)
        for storage in storages:
            if isinstance(storage.get_schema(), TableSchema):
                cluster = storage.get_cluster()
                connection_grouped_table_names[cluster.get_connection_id()].add(
                    cast(TableSchema, storage.get_schema()).get_table_name()
                )
        # De-dupe clusters by host:TCP port:HTTP port:database
        unique_clusters = {
            storage.get_cluster().get_connection_id(): storage.get_cluster() for storage in storages
        }

        for cluster_key, cluster in unique_clusters.items():
            clickhouse = cluster.get_query_connection(ClickhouseClientSettings.QUERY)
            clickhouse_tables = clickhouse.execute("show tables").results
            known_table_names = connection_grouped_table_names[cluster_key]
            logger.debug(f"checking for {known_table_names} on {cluster_key}")
            for table in known_table_names:
                if (table,) not in clickhouse_tables:
                    logger.error(f"{table} not present in cluster {cluster}")
                    if metric_tags is not None:
                        metric_tags["table_not_present"] = table
                        metric_tags["cluster"] = str(cluster)
                    return False

        return True

    except UndefinedClickhouseCluster as err:
        if metric_tags is not None and isinstance(err.extra_data, dict):
            # Be a little defensive here, since it's not always obvious this extra data
            # is being passed to metrics, and we might want non-string values in the
            # exception data.
            for k, v in err.extra_data.items():
                if isinstance(v, str):
                    metric_tags[k] = v

        logger.error(err)
        return False

    except ClickhouseError as err:
        if metric_tags and err.__cause__:
            metric_tags["exception"] = type(err.__cause__).__name__
        logger.error(err)
        return False

    except Exception as err:
        logger.error(err)
        return False
