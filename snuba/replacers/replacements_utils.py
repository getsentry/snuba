from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Mapping, Sequence

logger = logging.getLogger(__name__)


from snuba.redis import RedisClientKey, get_redis_client

redis_client = get_redis_client(RedisClientKey.REPLACEMENTS_STORE)
config_auto_replacements_bypass_projects_hash = (
    "snuba-config-auto-replacements-bypass-projects-hash"
)
EXPIRY_WINDOW = timedelta(minutes=5)


def set_config_auto_replacements_bypass_projects(
    new_project_ids: Sequence[int], curr_time: datetime
) -> None:
    try:
        _filter_projects_within_expiry(curr_time)
        projects_within_expiry = _retrieve_projects_from_redis()
        for project_id in new_project_ids:
            if project_id not in projects_within_expiry:
                expiry = curr_time + EXPIRY_WINDOW
                redis_client.hset(
                    config_auto_replacements_bypass_projects_hash,
                    project_id,
                    expiry.isoformat(),
                )
    except Exception as e:
        logger.exception(e)


def get_config_auto_replacements_bypass_projects(
    curr_time: datetime,
) -> Mapping[int, datetime]:
    _filter_projects_within_expiry(curr_time)
    return _retrieve_projects_from_redis()


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


def _filter_projects_within_expiry(curr_time: datetime) -> None:
    curr_projects = _retrieve_projects_from_redis()

    for project_id in curr_projects:
        if curr_projects[project_id] < curr_time:
            redis_client.hdel(config_auto_replacements_bypass_projects_hash, project_id)
