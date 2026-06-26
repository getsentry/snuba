from __future__ import annotations

from collections.abc import Sequence
from typing import NamedTuple, TypedDict

from snuba import settings
from snuba.datasets.slicing import is_storage_set_sliced
from snuba.datasets.storage import WritableTableStorage
from snuba.datasets.storages.factory import get_writable_storages


class Topic(TypedDict):
    logicalName: str
    physicalName: str
    slice: int | None
    storage: str


class DlqTopic(NamedTuple):
    logical_name: str
    physical_name: str
    slice_id: int | None
    storage: str

    def to_json(self) -> Topic:
        return {
            "logicalName": self.logical_name,
            "physicalName": self.physical_name,
            "slice": self.slice_id,
            "storage": self.storage,
        }


def get_dlq_topics() -> Sequence[Topic]:
    dlq_topics = []

    storages = get_writable_storages()
    for storage in storages:
        stream_loader = storage.get_table_writer().get_stream_loader()
        dlq_topic_spec = stream_loader.get_dlq_topic_spec()
        if dlq_topic_spec is not None:
            for slice_id in get_slices(storage):
                logical_name = dlq_topic_spec.topic.value
                physical_name = dlq_topic_spec.get_physical_topic_name(slice_id)
                dlq_topics.append(
                    DlqTopic(
                        logical_name,
                        physical_name,
                        slice_id,
                        storage.get_storage_key().value,
                    )
                )

    return [t.to_json() for t in dlq_topics]


def get_slices(storage: WritableTableStorage) -> Sequence[int | None]:
    storage_set_key = storage.get_storage_set_key()

    if is_storage_set_sliced(storage_set_key):
        return list(range(settings.SLICED_STORAGE_SETS[storage_set_key.value]))
    return [None]
