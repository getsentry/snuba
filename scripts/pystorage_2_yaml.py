from __future__ import annotations

import sys
from typing import Any, Sequence

import yaml

from snuba.datasets.configuration.utils import serialize_columns
from snuba.datasets.plans.splitters import QuerySplitStrategy
from snuba.datasets.schemas import Schema
from snuba.datasets.storage import WritableStorage, WritableTableStorage
from snuba.datasets.storages.factory import get_storage, initialize_storage_factory
from snuba.datasets.storages.storage_key import StorageKey
from snuba.datasets.table_storage import KafkaStreamLoader
from snuba.query.processors.physical import ClickhouseQueryProcessor
from snuba.utils.registered_class import RegisteredClass

initialize_storage_factory()


def _convert_registered_class(cls: Any, name: str):
    res = {}
    res[name] = cls.config_key()
    if cls.init_kwargs:
        res["args"] = cls.init_kwargs
    return res


def _convert_registered_classes(
    cls_list: Sequence[Any], name: str
) -> list[dict[str, Any]]:
    res = []
    for cls in cls_list:
        res.append(_convert_registered_class(cls, name))

    return res


def _convert_stream_loader(stream_loader: KafkaStreamLoader):
    res = {
        "processor": _convert_registered_class(stream_loader.get_processor(), "name"),
        "default_topic": stream_loader.get_default_topic_spec().topic_name,
    }

    if stream_loader.get_commit_log_topic_spec() is not None:
        res["commit_log_topic"] = stream_loader.get_commit_log_topic_spec().topic_name
    if stream_loader.get_subscription_scheduler_mode():
        res[
            "subscription_scheduler_mode"
        ] = stream_loader.get_subscription_scheduler_mode()
    if stream_loader.get_subscription_scheduled_topic_spec() is not None:
        res["subscription_scheduled_topic"] = (
            stream_loader.get_subscription_scheduled_topic_spec().topic_name,
        )
    if stream_loader.get_subscription_result_topic_spec() is not None:
        res[
            "subscription_result_topic"
        ] = stream_loader.get_subscription_result_topic_spec().topic_name
    return res


def _convert_schema(schema: Schema):
    res = {
        "columns": serialize_columns(schema.get_columns().columns),
        "local_table_name": schema.get_local_table_name(),
        "dist_table_name": schema.get_table_name(),
    }
    if schema.get_partition_format():
        res["partition_format"] = schema.get_partition_format()
    return res


def convert_to_yaml(key: StorageKey, result_path):
    storage = get_storage(key)
    res = {
        "version": "v1",
        "kind": "writable_storage"
        if isinstance(storage, WritableStorage)
        else "readable_storage",
        "name": key.value,
        "storage": {"key": key.value, "set_key": storage.get_storage_set_key().value},
    }
    res["schema"] = _convert_schema(storage.get_schema())
    res["query_processors"] = _convert_registered_classes(
        storage.get_query_processors(), "processor"
    )
    res["query_splitters"] = _convert_registered_classes(
        storage.get_query_splitters(), "splitter"
    )
    res["mandatory_condition_checkers"] = _convert_registered_classes(
        storage.get_mandatory_condition_checkers(), "condition"
    )
    if isinstance(storage, WritableTableStorage):
        writer_options = storage.get_table_writer()._TableWriter__writer_options
        if writer_options:
            res["writer_options"] = writer_options
        stream_loader = storage.get_table_writer().get_stream_loader()
        res["stream_loader"] = _convert_stream_loader(stream_loader)
    with open(result_path, "w") as f:
        yaml.dump(res, f, sort_keys=False)


if __name__ == "__main__":
    convert_to_yaml(StorageKey(sys.argv[1]), sys.argv[2])
