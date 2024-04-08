from __future__ import annotations

import itertools
import logging
import os
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, MutableMapping, Optional, Set, Union, cast

import simplejson as json

from snuba import environment, settings
from snuba.clickhouse.errors import ClickhouseError
from snuba.clusters.cluster import (
    ClickhouseClientSettings,
    ConnectionId,
    UndefinedClickhouseCluster,
)
from snuba.datasets.factory import get_dataset, get_enabled_dataset_names
from snuba.datasets.schemas.tables import TableSchema
from snuba.datasets.storage import Storage
from snuba.environment import setup_logging
from snuba.utils.metrics.wrapper import MetricsWrapper

metrics = MetricsWrapper(environment.metrics, "api")
setup_logging(None)
logger = logging.getLogger("snuba.health")


def shutdown_time() -> Optional[float]:
    try:
        return os.stat("/tmp/snuba.down").st_mtime
    except OSError:
        return None


_IS_SHUTTING_DOWN = False

try:
    import uwsgi
except ImportError:

    def check_down_file_exists() -> bool:
        return False

else:

    def check_down_file_exists() -> bool:
        try:
            m_time = shutdown_time()
            if m_time is None:
                return False

            start_time: float = uwsgi.started_on
            _set_shutdown(m_time > start_time)
            return get_shutdown()
        except OSError:
            return False


def _set_shutdown(is_shutting_down: bool) -> None:
    global _IS_SHUTTING_DOWN
    _IS_SHUTTING_DOWN = is_shutting_down


def get_shutdown() -> bool:
    return _IS_SHUTTING_DOWN


@dataclass
class HealthInfo:
    body: str
    status: int
    content_type: Dict[str, str]


def get_health_info(thorough: Union[bool, str]) -> HealthInfo:

    start = time.time()
    down_file_exists = check_down_file_exists()

    metric_tags = {
        "down_file_exists": str(down_file_exists),
        "thorough": str(thorough),
    }

    clickhouse_health = check_clickhouse(metric_tags=metric_tags) if thorough else True
    metric_tags["clickhouse_ok"] = str(clickhouse_health)

    body: Mapping[str, Union[str, bool]]
    if clickhouse_health:
        body = {"status": "ok", "down_file_exists": down_file_exists}
        status = 200
    else:
        body = {
            "down_file_exists": down_file_exists,
        }
        if thorough:
            body["clickhouse_ok"] = clickhouse_health
        status = 502

    if status != 200:
        metrics.increment("healthcheck_failed", tags=metric_tags)
        logger.error(f"Snuba health check failed! Tags: {metric_tags}")
    metrics.timing(
        "healthcheck.latency", time.time() - start, tags={"thorough": str(thorough)}
    )

    logger.info(json.dumps(body))
    return HealthInfo(
        body=json.dumps(body),
        status=status,
        content_type={"Content-Type": "application/json"},
    )


def filter_checked_storages() -> List[Storage]:
    datasets = [get_dataset(name) for name in get_enabled_dataset_names()]
    entities = itertools.chain(*[dataset.get_all_entities() for dataset in datasets])

    storages: List[Storage] = []
    for entity in entities:
        for storage in entity.get_all_storages():
            if storage.get_readiness_state().value in settings.SUPPORTED_STATES:
                storages.append(storage)
    return storages


def check_clickhouse(metric_tags: dict[str, Any] | None = None) -> bool:
    """
    Checks if all the tables in all the enabled datasets exist in ClickHouse
    TODO: Eventually, when we fully migrate to readiness_states, we can remove DISABLED_DATASETS.
    """

    setup_logging(None)
    logger = logging.getLogger("snuba.health")

    try:
        storages = filter_checked_storages()
        connection_grouped_table_names: MutableMapping[
            ConnectionId, Set[str]
        ] = defaultdict(set)
        for storage in storages:
            if isinstance(storage.get_schema(), TableSchema):
                cluster = storage.get_cluster()
                connection_grouped_table_names[cluster.get_connection_id()].add(
                    cast(TableSchema, storage.get_schema()).get_table_name()
                )
        # De-dupe clusters by host:TCP port:HTTP port:database
        unique_clusters = {
            storage.get_cluster().get_connection_id(): storage.get_cluster()
            for storage in storages
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
