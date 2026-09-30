"""DirectRunner tests for the transform portion of the pipeline (no Kafka/BigQuery)."""

import base64
import json
import uuid

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.utils.timestamp import Timestamp as BeamTimestamp
from eap_items_dataflow.pipeline import (
    BQ_SCHEMA,
    DEAD_LETTER,
    EapItemsOptions,
    TraceItemToRow,
    kafka_consumer_config,
    to_storage_write_row,
)
from eap_items_dataflow.transform import TransformConfig, keep_trace
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.request_common_pb2 import TraceItemType
from sentry_protos.snuba.v1.trace_item_pb2 import AnyValue, TraceItem


def direct_pipeline():
    return BeamTestPipeline(
        runner="DirectRunner", options=PipelineOptions(["--direct_num_workers=1"])
    )


def item_bytes(trace_id: str, n: int, item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN) -> bytes:
    return TraceItem(
        organization_id=1,
        project_id=2,
        trace_id=trace_id,
        item_id=n.to_bytes(16, "little"),
        item_type=item_type,
        timestamp=Timestamp(seconds=1_700_000_000),
        attributes={"n": AnyValue(int_value=n)},
    ).SerializeToString()


def test_direct_runner_rows_and_dead_letters():
    good = [item_bytes(uuid.UUID(int=i).hex, i + 1) for i in range(5)]
    bad = [b"\xff\xff\xff", item_bytes("nope", 1)]
    with direct_pipeline() as p:
        out = (
            p
            | beam.Create(good + bad)
            | beam.ParDo(TraceItemToRow(TransformConfig())).with_outputs(DEAD_LETTER, main="rows")
        )
        assert_that(
            out.rows
            | "ids" >> beam.Map(lambda r: (r["item_id"], r["attributes"], r["sample_rate"])),
            equal_to(
                [
                    (format(i + 1, "016x"), json.dumps({"n": i + 1}, separators=(",", ":")), 1.0)
                    for i in range(5)
                ]
            ),
            label="rows",
        )
        assert_that(
            out[DEAD_LETTER]
            | "dl" >> beam.Map(lambda d: (d["stage"], base64.b64decode(d["payload_b64"]))),
            equal_to([("transform", b) for b in bad]),
            label="dead_letters",
        )


def test_direct_runner_sampling_keeps_whole_traces():
    config = TransformConfig(sample_rate=0.5)
    traces = [uuid.UUID(int=i * 104729).hex for i in range(40)]
    payloads = [
        item_bytes(t, j, item_type)
        for t in traces
        for j, item_type in enumerate(
            [
                TraceItemType.TRACE_ITEM_TYPE_SPAN,
                TraceItemType.TRACE_ITEM_TYPE_LOG,
                TraceItemType.TRACE_ITEM_TYPE_ERROR,
            ],
            start=1,
        )
    ]
    expected = sorted((t, n) for t in traces if keep_trace(t, config) for n in (1, 2, 3))
    assert 0 < len(expected) < len(payloads)
    with direct_pipeline() as p:
        rows = p | beam.Create(payloads) | beam.ParDo(TraceItemToRow(config))
        assert_that(
            rows | beam.Map(lambda r: (r["trace_id"], int(r["item_id"], 16))),
            equal_to(expected),
        )


def test_storage_write_row_conversion_matches_schema():
    from apache_beam.io.gcp import bigquery_tools

    row = next(TraceItemToRow(TransformConfig()).process(item_bytes(uuid.uuid4().hex, 7)))
    sw = to_storage_write_row(row)
    assert sw["timestamp"] == BeamTimestamp(1_700_000_000)
    beam_row = bigquery_tools.beam_row_from_dict(sw, BQ_SCHEMA)
    hints = dict(bigquery_tools.get_beam_typehints_from_tableschema(BQ_SCHEMA, {"JSON": str}))
    assert set(hints) == set(beam_row.as_dict()) == {f["name"] for f in BQ_SCHEMA["fields"]}
    assert isinstance(beam_row.attributes, str)


def test_schema_matches_ops_items_schema():
    """Guard against drift from ops/terragrunt/.../bigquery/eap/items_schema.hcl."""
    expected = [
        ("organization_id", "STRING", "REQUIRED"),
        ("project_id", "STRING", "REQUIRED"),
        ("item_type", "STRING", "REQUIRED"),
        ("timestamp", "TIMESTAMP", "REQUIRED"),
        ("trace_id", "STRING", "NULLABLE"),
        ("item_id", "STRING", "REQUIRED"),
        ("attributes", "JSON", "NULLABLE"),
        ("retention_days", "INTEGER", "NULLABLE"),
        ("sample_rate", "FLOAT", "NULLABLE"),
    ]
    assert [(f["name"], f["type"], f["mode"]) for f in BQ_SCHEMA["fields"]] == expected


def test_options_defaults_and_kafka_config():
    opts = PipelineOptions(["--bootstrap_servers=a:9092,b:9092"]).view_as(EapItemsOptions)
    assert opts.topic == "snuba-items"
    assert opts.consumer_group_id == "eap-bigquery-items"
    assert opts.output_table == "sentry-s4s2:eap.items"
    assert opts.sample_rate == 1.0
    assert opts.start_offset == "latest"
    assert opts.write_method == "STORAGE_WRITE_API"
    cfg = kafka_consumer_config(opts)
    assert cfg["bootstrap.servers"] == "a:9092,b:9092"
    assert cfg["group.id"] == "eap-bigquery-items"
    assert cfg["auto.offset.reset"] == "latest"

    opts = PipelineOptions(
        [
            "--bootstrap_servers=x:1",
            "--start_offset=earliest",
            '--kafka_consumer_config={"max.poll.records": 1000}',
        ]
    ).view_as(EapItemsOptions)
    cfg = kafka_consumer_config(opts)
    assert cfg["auto.offset.reset"] == "earliest"
    assert cfg["max.poll.records"] == "1000"


def test_dead_letter_files_written(tmp_path):
    from eap_items_dataflow.pipeline import handle_dead_letters

    opts = PipelineOptions(
        ["--bootstrap_servers=x:1", f"--dead_letter_path={tmp_path}/dl/"]
    ).view_as(EapItemsOptions)
    with direct_pipeline() as p:
        out = (
            p
            | beam.Create([b"\xff\xff\xff", b""])
            # Kafka records carry event timestamps; Create's MIN_TIMESTAMP
            # would overflow windowed file naming.
            | beam.Map(lambda x: beam.window.TimestampedValue(x, 1_700_000_000))
            | beam.ParDo(TraceItemToRow(TransformConfig())).with_outputs(DEAD_LETTER, main="rows")
        )
        handle_dead_letters(out[DEAD_LETTER], opts)
    files = [f for f in (tmp_path / "dl").rglob("dead-letter*.jsonl") if f.is_file()]
    lines = [json.loads(line) for f in files for line in f.read_text().splitlines()]
    assert sorted(rec["error"].split(":")[0] for rec in lines) == [
        "empty payload",
        "protobuf decode error",
    ]
