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
from typing import Optional, Sequence

import rapidjson

module_object = importlib.import_module(os.environ["RUST_SNUBA_PROCESSOR_MODULE"])
Processor = getattr(module_object, os.environ["RUST_SNUBA_PROCESSOR_CLASSNAME"])

processor = Processor.from_kwargs()

from snuba.consumers.consumer import json_row_encoder
from snuba.consumers.types import KafkaMessageMetadata
from snuba.processor import InsertBatch


def process_rust_message(
    message: bytes, offset: int, partition: int, timestamp: datetime
) -> Optional[Sequence[bytes]]:
    rv = processor.process_message(
        rapidjson.loads(bytearray(message)),
        KafkaMessageMetadata(offset=offset, partition=partition, timestamp=timestamp),
    )

    if rv is None:
        return rv

    assert isinstance(rv, InsertBatch), "this consumer does not support replacements"

    return [json_row_encoder.encode(row) for row in rv.rows]
