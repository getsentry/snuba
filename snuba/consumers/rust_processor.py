# This file is loaded from within Rust in a subprocess with certain environment
# variables:
#
# python interpreter
#   runs: rust snuba CLI
#     calls: rust_snuba crate's consumer main function, exposed via PyO3
#       spawns: N subprocesses via fork()
#         sets environment variables and imports: this module
#
# importing this file at the outer subprocess will crash due to missing
# environment variables


import importlib
import os
from datetime import datetime
from typing import Optional, Sequence, Type

import rapidjson

from snuba.consumers.consumer import json_row_encoder
from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.processors import DatasetMessageProcessor
from snuba.processor import InsertBatch

processor: Optional[DatasetMessageProcessor] = None


def initialize_processor(
    module: Optional[str] = None, classname: Optional[str] = None
) -> None:
    if not module or not classname:
        module = os.environ.get("RUST_SNUBA_PROCESSOR_MODULE")
        classname = os.environ.get("RUST_SNUBA_PROCESSOR_CLASSNAME")

    if not module or not classname:
        return

    module_object = importlib.import_module(module)
    Processor: Type[DatasetMessageProcessor] = getattr(module_object, classname)

    global processor
    processor = Processor()


initialize_processor()


def process_rust_message(
    message: bytes, offset: int, partition: int, timestamp: datetime
) -> Optional[Sequence[bytes]]:
    if processor is None:
        raise RuntimeError("processor not yet initialized")
    rv = processor.process_message(
        rapidjson.loads(bytearray(message)),
        KafkaMessageMetadata(offset=offset, partition=partition, timestamp=timestamp),
    )

    if rv is None:
        return rv

    assert isinstance(rv, InsertBatch), "this consumer does not support replacements"

    return [json_row_encoder.encode(row) for row in rv.rows]
