import json
import time
from datetime import datetime, timedelta
from random import Random
from uuid import UUID

from snuba import settings
from snuba.utils.streams.kafka import KafkaPayload


def generate_insertion_event(random: Random, scale: int = 1) -> KafkaPayload:
    project_id = random.randint(1, scale)
    platform = random.choice(["python", "javascript"])
    data = {
        "event_id": UUID(int=random.randint(0, 2 ** 128)).hex,
        "project_id": project_id,
        "group_id": ((project_id - 1) * (10 * scale)) + random.randint(0, 10 * scale),
        "datetime": (  # TODO: This could follow a more reasonable distribution than uniform.
            datetime.utcnow() - timedelta(seconds=random.randint(0, 120))
        ).strftime(
            settings.PAYLOAD_DATETIME_FORMAT
        ),
        "platform": platform,
        "primary_hash": UUID(int=random.randint(0, 2 ** 128)).hex,
        "data": {"received": time.time() - random.random() * 30},
        "message": "hi",
    }
    return KafkaPayload(
        str(data["project_id"]).encode("utf-8"),
        json.dumps([2, "insert", data]).encode("utf-8"),
    )


if __name__ == "__main__":
    while True:
        print(generate_insertion_event(Random(), 10000))
