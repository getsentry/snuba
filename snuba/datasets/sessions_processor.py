import uuid
from datetime import datetime
from typing import Optional

from snuba import environment
from snuba.processor import (
    MAX_UINT32,
    NIL_UUID,
    InsertBatch,
    MessageProcessor,
    ProcessedMessage,
    _collapse_uint16,
    _collapse_uint32,
    _ensure_valid_date,
)
from snuba.utils.metrics.wrapper import MetricsWrapper

STATUS_MAPPING = {
    "ok": 0,
    "exited": 1,
    "crashed": 2,
    "abnormal": 3,
    "errored": 4,
}

metrics = MetricsWrapper(environment.metrics, "sessions.processor")


class SessionsProcessor(MessageProcessor):
    def process_message(self, message, metadata) -> Optional[ProcessedMessage]:
        # some old relays accidentally emit rows without release
        if message["release"] is None:
            return None

        received = _ensure_valid_date(datetime.utcfromtimestamp(message["received"]))
        if received is None:
            metrics.increment("empty_received_date")

        if message.get("aggregates") is not None:
            template = {
                "session_id": NIL_UUID,
                "seq": 0,
                "org_id": message["org_id"],
                "project_id": message["project_id"],
                "retention_days": message["retention_days"],
                "duration": MAX_UINT32,
                "received": received if received is not None else datetime.now(),
                "release": message["release"],
                "environment": message.get("environment") or "",
            }

            batch = []
            for group in message["aggregates"]:
                started = _ensure_valid_date(
                    datetime.utcfromtimestamp(group["started"])
                )
                if started is None:
                    continue

                group_template = dict(
                    template,
                    distinct_id=str(uuid.UUID(group.get("distinct_id") or NIL_UUID)),
                    started=started,
                )

                if group.get("exited") is not None:
                    batch.append(
                        dict(
                            group_template,
                            status=STATUS_MAPPING["exited"],
                            errors=0,
                            quantity=group["exited"],
                        )
                    )
                if group.get("errored") is not None:
                    batch.append(
                        dict(
                            group_template,
                            status=STATUS_MAPPING["errored"],
                            errors=1,
                            quantity=group["errored"],
                        )
                    )
                if group.get("abnormal") is not None:
                    batch.append(
                        dict(
                            group_template,
                            status=STATUS_MAPPING["abnormal"],
                            errors=1,
                            quantity=group["abnormal"],
                        )
                    )
                if group.get("crashed") is not None:
                    batch.append(
                        dict(
                            group_template,
                            status=STATUS_MAPPING["crashed"],
                            errors=1,
                            quantity=group["crashed"],
                        )
                    )

            return InsertBatch(batch)

        if message["duration"] is None:
            duration = None
        else:
            duration = _collapse_uint32(int(message["duration"] * 1000))

        # since duration is not nullable, the max duration means no duration
        if duration is None:
            duration = MAX_UINT32

        errors = _collapse_uint16(message["errors"]) or 0

        # If a session ends in crashed or abnormal we want to make sure that
        # they count as errored too, so we can get the number of health and
        # errored sessions correctly.
        if message["status"] in ("crashed", "abnormal"):
            errors = max(errors, 1)

        started = _ensure_valid_date(datetime.utcfromtimestamp(message["started"]))

        if started is None:
            metrics.increment("empty_started_date")

        processed = {
            "session_id": str(uuid.UUID(message["session_id"])),
            "distinct_id": str(uuid.UUID(message.get("distinct_id") or NIL_UUID)),
            "quantity": 1,
            "seq": message["seq"],
            "org_id": message["org_id"],
            "project_id": message["project_id"],
            "retention_days": message["retention_days"],
            "duration": duration,
            "status": STATUS_MAPPING[message["status"]],
            "errors": errors,
            "received": received if received is not None else datetime.now(),
            "started": started if started is not None else datetime.now(),
            "release": message["release"],
            "environment": message.get("environment") or "",
        }
        return InsertBatch([processed])
