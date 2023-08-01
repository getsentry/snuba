from typing import Any, Dict

from flask import Response

from celery import Celery
from snuba import settings
from snuba.datasets.dataset import Dataset
from snuba.utils.metrics.timer import Timer
from snuba.web.views import dataset_query


def init_celery() -> Celery:
    redis_node = settings.REDIS_CLUSTERS["async_queries"]
    if redis_node:
        password = redis_node["password"]
        host = redis_node["host"]
        port = redis_node["port"]
        db_number = redis_node["db"]
    else:
        password = settings.REDIS_PASSWORD
        host = settings.REDIS_HOST
        port = settings.REDIS_PORT
        db_number = settings.REDIS_DB

    if password:
        redis_url = f"redis://:{password}@{host}:{port}/{db_number}"
    else:
        redis_url = f"redis://{host}:{port}/{db_number}"
    celery_app = Celery(__name__)
    celery_app.conf.broker_url = redis_url
    celery_app.conf.result_backend = redis_url
    return celery_app


celery_app = init_celery()


@celery_app.task
def send_query(dataset: Dataset, body: Dict[str, Any], timer: Timer) -> Response:
    return dataset_query(dataset, body, timer)
