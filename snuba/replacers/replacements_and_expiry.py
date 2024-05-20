from __future__ import annotations

import logging
import time
import typing
from datetime import datetime, timedelta
from typing import Mapping, Sequence

logger = logging.getLogger(__name__)


from snuba import environment
from snuba.redis import RedisClientKey, get_redis_client
from snuba.state import get_int_config
from snuba.utils.metrics.wrapper import MetricsWrapper

metrics = MetricsWrapper(environment.metrics, "replacements_and_expiry")

redis_client = get_redis_client(RedisClientKey.REPLACEMENTS_STORE)
config_auto_replacements_bypass_projects_hash = (
    "snuba-config-auto-replacements-bypass-projects-hash"
)

REPLACEMENTS_EXPIRY_WINDOW_MINUTES_KEY = "replacements_expiry_window_minutes"


def set_config_auto_replacements_bypass_projects(
    new_project_ids: Sequence[int], curr_time: datetime
) -> None:
    try:
        projects_within_expiry = get_config_auto_replacements_bypass_projects(curr_time)
        start = time.time()
        expiry_window = typing.cast(
            int, get_int_config(key=REPLACEMENTS_EXPIRY_WINDOW_MINUTES_KEY, default=5)
        )
        pipeline = redis_client.pipeline()
        for project_id in new_project_ids:
            if project_id not in projects_within_expiry:
                expiry = curr_time + timedelta(minutes=expiry_window)
                pipeline.hset(
                    config_auto_replacements_bypass_projects_hash,
                    project_id,
                    expiry.isoformat(),
                )
        pipeline.execute()

        metrics.timing(
            "set_config_auto_replacements_bypass_projects_duration",
            time.time() - start,
            tags={},
        )
    except Exception as e:
        logger.exception(e)


def _retrieve_projects_from_redis() -> Mapping[int, datetime]:
    try:
        start = time.time()
        projects = {
            int(k.decode("utf-8")): datetime.fromisoformat(v.decode("utf-8"))
            for k, v in redis_client.hgetall(
                config_auto_replacements_bypass_projects_hash
            ).items()
        }
        metrics.timing(
            "retrieve_projects_from_redis_duration",
            time.time() - start,
            tags={},
        )
        return projects
    except Exception as e:
        logger.exception(e)
        return {}


def get_config_auto_replacements_bypass_projects(
    curr_time: datetime,
) -> Mapping[int, datetime]:
    curr_projects = _retrieve_projects_from_redis()
    start = time.time()
    valid_projects = {}
    pipeline = redis_client.pipeline()
    for project_id in curr_projects:
        if curr_projects[project_id] < curr_time:
            pipeline.hdel(config_auto_replacements_bypass_projects_hash, project_id)
        else:
            valid_projects[project_id] = curr_projects[project_id]
    pipeline.execute()
    metrics.timing(
        "deleting_expired_projects",
        time.time() - start,
        tags={},
    )
    return valid_projects
