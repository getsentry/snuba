"""Beam pipeline: Kafka snuba-items -> TraceItem -> BigQuery eap.items."""

from __future__ import annotations

import base64
import datetime as dt
import json
import logging
import traceback
from collections.abc import Iterable
from typing import Any

import apache_beam as beam
from apache_beam.io.gcp.bigquery import BigQueryDisposition, WriteToBigQuery
from apache_beam.metrics import Metrics
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions
from apache_beam.utils.timestamp import Timestamp

from eap_items_dataflow.transform import (
    InvalidTraceItem,
    TransformConfig,
    decode_trace_item,
    trace_item_to_row,
)

LOG = logging.getLogger(__name__)

DEAD_LETTER = "dead_letter"
METRICS_NAMESPACE = "eap_items"

WRITE_METHODS = ("STORAGE_WRITE_API", "STORAGE_WRITE_API_EXACTLY_ONCE", "STREAMING_INSERTS")
START_OFFSETS = ("latest", "earliest")

# Must match ops/terragrunt/regions/multi-tenant/bigquery/eap/items_schema.hcl.
# The table is created by Terraform; the pipeline never creates or alters it.
BQ_SCHEMA = {
    "fields": [
        {"name": "organization_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "project_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "item_type", "type": "STRING", "mode": "REQUIRED"},
        {"name": "timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "trace_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "item_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "attributes", "type": "JSON", "mode": "NULLABLE"},
        {"name": "retention_days", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "sample_rate", "type": "FLOAT", "mode": "NULLABLE"},
    ]
}


class EapItemsOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            "--bootstrap_servers",
            default="",
            help="Comma-separated Kafka brokers (host:port). Required.",
        )
        parser.add_argument("--topic", default="snuba-items")
        parser.add_argument("--consumer_group_id", default="eap-bigquery-items")
        parser.add_argument(
            "--output_table", default="sentry-s4s2:eap.items", help="PROJECT:DATASET.TABLE"
        )
        parser.add_argument(
            "--sample_rate",
            type=float,
            default=1.0,
            help="Pipeline-side trace sampling rate in (0, 1].",
        )
        parser.add_argument(
            "--sample_seed", default="", help="Salt for the trace_id sampling hash."
        )
        parser.add_argument(
            "--start_offset",
            default="latest",
            choices=START_OFFSETS,
            help="Kafka auto.offset.reset: where to start when the consumer "
            "group has no committed offset. Committed offsets always win.",
        )
        parser.add_argument(
            "--start_read_time_ms",
            type=int,
            default=-1,
            help="If >= 0, ignore committed offsets and start every partition "
            "at this broker timestamp (epoch ms).",
        )
        parser.add_argument(
            "--write_method",
            default="STORAGE_WRITE_API",
            choices=WRITE_METHODS,
            help="STORAGE_WRITE_API = at-least-once Storage Write API.",
        )
        parser.add_argument(
            "--triggering_frequency_seconds",
            type=int,
            default=5,
            help="Commit interval for STORAGE_WRITE_API_EXACTLY_ONCE.",
        )
        parser.add_argument("--retention_default_days", type=int, default=30)
        parser.add_argument("--retention_max_days", type=int, default=90)
        parser.add_argument(
            "--dead_letter_path",
            default="",
            help="Optional GCS prefix for dead-letter records "
            "(gs://bucket/path). Empty = log + metric only.",
        )
        parser.add_argument("--dead_letter_window_seconds", type=int, default=300)
        parser.add_argument(
            "--kafka_consumer_config",
            default="{}",
            help="JSON object of extra Kafka consumer properties.",
        )
        parser.add_argument(
            "--max_num_records",
            type=int,
            default=0,
            help="Testing only: stop after N records (0 = unbounded).",
        )


def dead_letter_record(payload: bytes, error: str, stage: str) -> dict[str, Any]:
    return {
        "stage": stage,
        "error": error,
        "payload_b64": base64.b64encode(payload or b"").decode("ascii"),
        "payload_size": len(payload or b""),
        "observed_at": dt.datetime.now(dt.UTC).isoformat(),
    }


class TraceItemToRow(beam.DoFn):
    """bytes -> eap.items row dict; failures go to the ``dead_letter`` output."""

    def __init__(self, config: TransformConfig):
        self.config = config
        self.processed = Metrics.counter(METRICS_NAMESPACE, "items_processed")
        self.written = Metrics.counter(METRICS_NAMESPACE, "rows_emitted")
        self.sampled_out = Metrics.counter(METRICS_NAMESPACE, "rows_sampled_out")
        self.invalid = Metrics.counter(METRICS_NAMESPACE, "dead_letter_invalid")
        self.errors = Metrics.counter(METRICS_NAMESPACE, "dead_letter_unexpected_error")
        self.payload_bytes = Metrics.distribution(METRICS_NAMESPACE, "payload_bytes")

    def process(self, payload: bytes) -> Iterable[Any]:
        self.processed.inc()
        self.payload_bytes.update(len(payload or b""))
        try:
            row = trace_item_to_row(decode_trace_item(payload), self.config)
        except InvalidTraceItem as e:
            self.invalid.inc()
            LOG.warning("dead-lettering invalid TraceItem: %s", e)
            yield beam.pvalue.TaggedOutput(
                DEAD_LETTER, dead_letter_record(payload, str(e), "transform")
            )
            return
        except Exception as e:  # noqa: BLE001 - never crash the streaming job on one message
            self.errors.inc()
            LOG.exception("dead-lettering TraceItem after unexpected error")
            yield beam.pvalue.TaggedOutput(
                DEAD_LETTER,
                dead_letter_record(
                    payload, f"{type(e).__name__}: {e}\n{traceback.format_exc()}", "transform"
                ),
            )
            return
        if row is None:
            self.sampled_out.inc()
            return
        self.written.inc()
        yield row


_EPOCH = dt.datetime(1970, 1, 1, tzinfo=dt.UTC)


def to_storage_write_row(row: dict[str, Any]) -> dict[str, Any]:
    """The Storage Write API cross-language path needs a Beam Timestamp for
    TIMESTAMP columns; the JSON column is sent as a string (type_overrides)."""
    out = dict(row)
    # Exact integer micros: Timestamp.from_rfc3339 goes through a float
    # (timedelta.total_seconds()) and can lose microseconds.
    when = dt.datetime.fromisoformat(row["timestamp"])
    out["timestamp"] = Timestamp(micros=(when - _EPOCH) // dt.timedelta(microseconds=1))
    return out


def build_transform_config(opts: EapItemsOptions) -> TransformConfig:
    return TransformConfig(
        sample_rate=opts.sample_rate,
        sample_seed=opts.sample_seed,
        retention_default_days=opts.retention_default_days,
        retention_max_days=opts.retention_max_days,
    )


def kafka_consumer_config(opts: EapItemsOptions) -> dict[str, str]:
    reset = opts.start_offset
    config = {
        "bootstrap.servers": opts.bootstrap_servers,
        "group.id": opts.consumer_group_id,
        "auto.offset.reset": reset,
        # Offsets are committed by KafkaIO on checkpoint finalization
        # (commit_offset_in_finalize) rather than by the Kafka client.
        "enable.auto.commit": "false",
    }
    extra = json.loads(opts.kafka_consumer_config or "{}")
    if not isinstance(extra, dict):
        raise ValueError("--kafka_consumer_config must be a JSON object")
    config.update({str(k): str(v) for k, v in extra.items()})
    return config


def read_from_kafka(p: beam.Pipeline, opts: EapItemsOptions) -> beam.PCollection:
    # Imported lazily so unit tests do not need Java / the expansion service.
    from apache_beam.io.kafka import ReadFromKafka

    kwargs: dict[str, Any] = {}
    if opts.start_read_time_ms >= 0:
        kwargs["start_read_time"] = opts.start_read_time_ms
    if opts.max_num_records:
        kwargs["max_num_records"] = opts.max_num_records

    return (
        p
        | "ReadFromKafka"
        >> ReadFromKafka(
            consumer_config=kafka_consumer_config(opts),
            topics=[opts.topic],
            commit_offset_in_finalize=True,
            # Processing time, not CreateTime: KafkaIO's external CreateTime
            # policy has zero allowed delay, so out-of-order producer
            # timestamps (many producers per partition) would become late data
            # and be silently dropped by the windowed dead-letter file writer.
            # Element timestamps are not used for anything else.
            timestamp_policy=ReadFromKafka.processing_time_policy,
            **kwargs,
        )
        | "DropKeys" >> beam.Map(lambda kv: kv[1])
    )


def write_to_bigquery(rows: beam.PCollection, opts: EapItemsOptions):
    common = {
        "table": opts.output_table,
        "schema": BQ_SCHEMA,
        "create_disposition": BigQueryDisposition.CREATE_NEVER,
        "write_disposition": BigQueryDisposition.WRITE_APPEND,
    }
    if opts.write_method.startswith("STORAGE_WRITE_API"):
        at_least_once = opts.write_method == "STORAGE_WRITE_API"
        return (
            rows
            | "ToStorageWriteRow" >> beam.Map(to_storage_write_row)
            | "WriteToBigQuery"
            >> WriteToBigQuery(
                method=WriteToBigQuery.Method.STORAGE_WRITE_API,
                use_at_least_once=at_least_once,
                triggering_frequency=None if at_least_once else opts.triggering_frequency_seconds,
                with_auto_sharding=not at_least_once,
                type_overrides={"JSON": str},
                **common,
            )
        )
    return rows | "WriteToBigQuery" >> WriteToBigQuery(
        method=WriteToBigQuery.Method.STREAMING_INSERTS,
        insert_retry_strategy="RETRY_ON_TRANSIENT_ERROR",
        **common,
    )


def handle_dead_letters(dead: beam.PCollection, opts: EapItemsOptions) -> None:
    lines = dead | "DeadLetterToJson" >> beam.Map(lambda r: json.dumps(r, sort_keys=True))
    if not opts.dead_letter_path:
        return
    from apache_beam.io import fileio
    from apache_beam.transforms import window

    (
        lines
        | "DeadLetterWindow"
        >> beam.WindowInto(window.FixedWindows(opts.dead_letter_window_seconds))
        | "WriteDeadLetters"
        >> fileio.WriteToFiles(
            path=opts.dead_letter_path.rstrip("/"),
            file_naming=fileio.default_file_naming("dead-letter", ".jsonl"),
            shards=1,
        )
    )


def failed_bq_rows_to_dead_letters(result, opts: EapItemsOptions) -> beam.PCollection:
    failed_bq = Metrics.counter(METRICS_NAMESPACE, "dead_letter_bigquery")

    def _to_record(row_and_error: Any) -> dict[str, Any]:
        failed_bq.inc()
        if isinstance(row_and_error, dict):
            row, error = row_and_error.get("failed_row"), row_and_error.get("error_message")
        else:  # STREAMING_INSERTS: (destination, row, errors)
            row, error = row_and_error[1], row_and_error[2] if len(row_and_error) > 2 else None
        LOG.warning("BigQuery rejected row: %s", error)
        return {
            "stage": "bigquery",
            "error": str(error),
            "row": json.dumps(row, default=str, sort_keys=True),
            "observed_at": dt.datetime.now(dt.UTC).isoformat(),
        }

    return result.failed_rows_with_errors | "BigQueryFailuresToDeadLetter" >> beam.Map(_to_record)


def build_pipeline(p: beam.Pipeline, opts: EapItemsOptions) -> None:
    config = build_transform_config(opts)
    payloads = read_from_kafka(p, opts)
    outputs = payloads | "TraceItemToRow" >> beam.ParDo(TraceItemToRow(config)).with_outputs(
        DEAD_LETTER, main="rows"
    )
    result = write_to_bigquery(outputs.rows, opts)
    bq_failures = failed_bq_rows_to_dead_letters(result, opts)
    dead = (outputs[DEAD_LETTER], bq_failures) | "MergeDeadLetters" >> beam.Flatten()
    handle_dead_letters(dead, opts)


def run(argv: list[str] | None = None) -> None:
    options = PipelineOptions(argv)
    opts = options.view_as(EapItemsOptions)
    options.view_as(StandardOptions).streaming = not opts.max_num_records
    options.view_as(SetupOptions).save_main_session = False
    if not opts.bootstrap_servers:
        raise ValueError("--bootstrap_servers is required")
    build_transform_config(opts)  # validate early, before submitting
    with beam.Pipeline(options=options) as p:
        build_pipeline(p, opts)
