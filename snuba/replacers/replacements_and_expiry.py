from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Mapping, Sequence

logger = logging.getLogger(__name__)


from snuba.redis import RedisClientKey, get_redis_client
from snuba.state import get_int_config

redis_client = get_redis_client(RedisClientKey.REPLACEMENTS_STORE)
config_auto_replacements_bypass_projects_hash = (
    "snuba-config-auto-replacements-bypass-projects-hash"
)

REPLACEMENTS_EXPIRY_WINDOW_MINUTES_KEY = "replacements_expiry_window_minutes"


def get_expiry_window_or_counter_window_size(key: str, default: int) -> int:
    minutes = get_int_config(key=key, default=default)
    return int(minutes) if minutes else default


def set_config_auto_replacements_bypass_projects(
    new_project_ids: Sequence[int], curr_time: datetime
) -> None:
    try:
        projects_within_expiry = get_config_auto_replacements_bypass_projects(curr_time)
        pipeline = redis_client.pipeline()
        for project_id in new_project_ids:
            if project_id not in projects_within_expiry:
                expiry = curr_time + timedelta(
                    minutes=get_expiry_window_or_counter_window_size(
                        REPLACEMENTS_EXPIRY_WINDOW_MINUTES_KEY, 5
                    )
                )
                pipeline.hset(
                    config_auto_replacements_bypass_projects_hash,
                    project_id,
                    expiry.isoformat(),
                )
        pipeline.execute()
    except Exception as e:
        logger.exception(e)


def _retrieve_projects_from_redis() -> Mapping[int, datetime]:
    try:
        return {
            int(k.decode("utf-8")): datetime.fromisoformat(v.decode("utf-8"))
            for k, v in redis_client.hgetall(
                config_auto_replacements_bypass_projects_hash
            ).items()
        }
    except Exception as e:
        logger.exception(e)
        return {}


def get_config_auto_replacements_bypass_projects(
    curr_time: datetime,
) -> Mapping[int, datetime]:
    curr_projects = _retrieve_projects_from_redis()

    valid_projects = {}
    pipeline = redis_client.pipeline()
    for project_id in curr_projects:
        if curr_projects[project_id] < curr_time:
            pipeline.hdel(config_auto_replacements_bypass_projects_hash, project_id)
        else:
            valid_projects[project_id] = pipeline.hget(
                config_auto_replacements_bypass_projects_hash, project_id
            )
    pipeline.execute()

    return valid_projects
