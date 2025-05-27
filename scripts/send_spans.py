# pylint: skip-file
# flake8: noqa

import datetime
import json
import random
import uuid
from functools import partial
from random import randint
from typing import Any, Callable

import click

"""
Traces across 3 services:

Frontend
Backend
API

Each service generates a transaction with between 10-100 spans
Each span has all the Sentry tags and some custom tags
Frontend spans have some measurements

Frontend makes 1-3 backend calls, which make 1-3 API calls

"""

PROJECTS = [1, 2, 3]
ORGANIZATION_ID = 1123
RECEIVED = datetime.datetime.now().timestamp()
CUSTOM_TAGS: list[tuple[str, Callable[[], Any]]] = [
    ("tag1", partial(random.randint, 0, 10)),
    ("tag2", lambda: str(uuid.uuid4())),
    ("tag3", partial(random.randint, 0, 100000)),
]

Span = dict[str, Any]
Producer = Callable[[list[Span], bool, bool], None]


def custom_tags(project_id: int) -> dict[str, str]:
    tags = {}
    for _ in range(random.randint(1, 5)):
        taggen = random.choice(CUSTOM_TAGS)
        tags[f"{taggen[0]}-{project_id}"] = str(taggen[1]())

    for i in range(random.randint(5, 100)):
        # Medium cardinality
        tags[f"tag{i}"] = f"value{i}_{random.randint(1, 1000000)}"

    return tags


def get_transaction_name(project_id: int) -> str:
    if project_id == 1:
        return f"/transaction_{randint(1, 3)}"
    elif project_id == 2:
        return f"transaction_2_{randint(1, 5)}"
    elif project_id == 3:
        return "database_transaction"

    raise ValueError(f"Unknown project_id: {project_id}")


ACTIONS = ["GET", "PUT", "POST", "DELETE"]
STATUS_CODES = ["200", "300", "400"]
OPS = ["db", "cache", "http"]


def sentry_tags(transaction_name: str) -> dict[str, str]:
    action = random.choice(ACTIONS)
    return {
        "http.method": action,
        "action": action,
        "domain": "test.sentry.net",
        "module": "module",
        "group": "group",
        "system": "system",
        "status": "status",
        "status_code": random.choice(STATUS_CODES),
        "transaction": transaction_name,
        "transaction.op": action,
        "op": random.choice(OPS),
        "transaction.method": action,
    }


def create_span(
    trace_id: str,
    parent_span_id: str | None,
    segment_id: str,
    is_segment: bool,
    project_id: int,
    duration_ms: int,
    start_timestamp: datetime.datetime,
    transaction_name: str,
    add_datetimes: bool = False,
) -> Span:
    end_timestamp = start_timestamp + datetime.timedelta(milliseconds=duration_ms)
    datetime_fields = {}
    if add_datetimes:
        datetime_fields = {
            "start_timestamp": start_timestamp.replace(microsecond=0).isoformat(),
            "end_timestamp": end_timestamp.replace(microsecond=0).isoformat(),
            "end_timestamp_ms": int(end_timestamp.timestamp() * 1000),
        }
    measurements = {
        "measurement1": {
            "value": random.random(),
            "unit": "unit",
        },
        "measurement2": {
            "value": random.randint(1, 100000),
            "unit": "unit",
        },
    }
    return {
        "event_id": uuid.uuid4().hex,
        "organization_id": ORGANIZATION_ID,
        "project_id": project_id,
        "trace_id": trace_id,
        "span_id": str(int(uuid.uuid4().hex[:8], 16)),
        "parent_span_id": parent_span_id,
        "segment_id": segment_id,
        "profile_id": uuid.uuid4().hex,
        "is_segment": is_segment,
        "start_timestamp_ms": int(start_timestamp.timestamp() * 1000),
        "start_timestamp_precise": start_timestamp.timestamp(),
        "end_timestamp_precise": end_timestamp.timestamp(),
        "duration_ms": duration_ms,
        "exclusive_time_ms": duration_ms,
        "retention_days": 90,
        "received": RECEIVED,
        "description": "This is a span",
        "tags": custom_tags(project_id),
        "sentry_tags": sentry_tags(transaction_name),
        "measurements": measurements,
        "measurements_flattened": {
            "measurement1": measurements["measurement1"]["value"],
            "measurement2": measurements["measurement2"]["value"],
        },
        **datetime_fields,
    }


def update_span_duration(span: Span, duration_ms: int) -> Span:
    start_timestamp = datetime.datetime.fromtimestamp(span["start_timestamp_precise"])
    end_timestamp = start_timestamp + datetime.timedelta(milliseconds=duration_ms)
    span["end_timestamp_precise"] = end_timestamp.timestamp()
    span["end_timestamp_ms"] = int(end_timestamp.timestamp() * 1000)
    span["end_timestamp"] = end_timestamp.replace(microsecond=0).isoformat()
    span["duration_ms"] = duration_ms + 10
    span["exclusive_time_ms"] = 10
    return span


def generate_span_branch(
    trace_id: str,
    project_id: int,
    start_timestamp: datetime.datetime,
    transaction_name: str,
    parent_span_id: str,
    span_count: int = 0,
    prefix: str = "",
    verbose: bool = False,
    add_datetimes: bool = False,
) -> tuple[list[Span], int]:
    duration = 0
    spans = []
    for i in range(randint(3, 10)):
        span_duration = randint(1, 50)
        span = create_span(
            trace_id=trace_id,
            parent_span_id=parent_span_id,
            segment_id=parent_span_id,
            is_segment=False,
            project_id=project_id,
            duration_ms=span_duration,
            start_timestamp=start_timestamp + datetime.timedelta(milliseconds=duration),
            transaction_name=transaction_name,
            add_datetimes=add_datetimes,
        )

        to_split_or_not_to_split = random.random()
        if span_count < 100 and to_split_or_not_to_split < 0.3:
            subspans: list[Span] = []
            subduration = 0
            if project_id != 3 and to_split_or_not_to_split < 0.15:
                if verbose:
                    print(f"{prefix}T{project_id}")
                subspans, subduration = create_transaction(
                    trace_id=trace_id,
                    project_id=project_id + 1,
                    start_timestamp=start_timestamp
                    + datetime.timedelta(milliseconds=duration),
                    parent_span_id=span["span_id"],
                    prefix=prefix + " ",
                    add_datetimes=add_datetimes,
                )
                if verbose:
                    print(f"{prefix}T /")

            else:
                if verbose:
                    print(f"{prefix} \\")
                subspans, subduration = generate_span_branch(
                    trace_id=trace_id,
                    project_id=project_id,
                    start_timestamp=start_timestamp
                    + datetime.timedelta(milliseconds=duration),
                    transaction_name=transaction_name,
                    parent_span_id=span["span_id"],
                    span_count=span_count,
                    prefix=prefix + " ",
                    add_datetimes=add_datetimes,
                )
                if verbose:
                    print(f"{prefix} /")

            # Update the span to have the durations of all its children
            update_span_duration(span, subduration)

            duration += subduration
            spans.extend(subspans)
            span_count += len(subspans)
        else:
            if verbose:
                print(f"{prefix}|")
            duration += span_duration

        span_count += 1
        spans.append(span)

    return spans, duration


def create_transaction(
    trace_id: str,
    project_id: int,
    start_timestamp: datetime.datetime,
    parent_span_id: str | None = None,
    prefix: str = "",
    add_datetimes: bool = False,
) -> tuple[list[dict[str, Any]], int]:
    transaction_name = get_transaction_name(project_id)
    root_span = create_span(
        trace_id=trace_id,
        parent_span_id=parent_span_id,
        segment_id=transaction_name,
        is_segment=True,
        project_id=project_id,
        duration_ms=0,
        start_timestamp=start_timestamp,
        transaction_name=transaction_name,
        add_datetimes=add_datetimes,
    )
    if root_span["parent_span_id"] is None:
        root_span["parent_span_id"] = root_span["span_id"]

    spans, duration = generate_span_branch(
        trace_id=trace_id,
        project_id=project_id,
        start_timestamp=start_timestamp,
        transaction_name=transaction_name,
        parent_span_id=root_span["span_id"],
        prefix=prefix,
        add_datetimes=add_datetimes,
    )
    update_span_duration(root_span, duration)
    return [root_span] + spans, duration


def create_trace(add_datetimes: bool) -> list[dict[str, Any]]:
    trace_id = uuid.uuid4().hex
    spans, _ = create_transaction(
        trace_id, 1, datetime.datetime.now(), add_datetimes=add_datetimes
    )
    return spans


def create_kafka_producer(host: str, topic: str) -> tuple[Producer, Any]:
    from arroyo.backends.kafka import KafkaPayload, KafkaProducer
    from arroyo.types import Topic

    conf = {"bootstrap.servers": host}

    kafka_producer = KafkaProducer(conf)

    def producer(messages: list[Span], dryrun: bool, verbose: bool) -> None:
        futures = []
        for i, message in enumerate(messages):
            if verbose:
                print(f"{i + 1} / {len(messages)}")
                print(json.dumps(message))
            if not dryrun:
                futures.append(
                    kafka_producer.produce(
                        Topic(name=(topic)),
                        KafkaPayload(
                            key=None,
                            value=json.dumps(message).encode("utf-8"),
                            headers=[],
                        ),
                    )
                )

        for future in futures:
            future.result()

    return producer, kafka_producer


def create_file_producer(filename: str) -> tuple[Producer, Any]:
    file_descriptor = open(filename, "a")

    def producer(messages: list[Span], dryrun: bool, verbose: bool) -> None:
        for i, message in enumerate(messages):
            if verbose:
                print(f"{i + 1} / {len(messages)}")
                print(json.dumps(message))
            if not dryrun:
                file_descriptor.write(json.dumps(message))
                file_descriptor.write("\n")

    return producer, file_descriptor


def write_to_stdout(messages: list[Span], dryrun: bool, verbose: bool) -> None:
    for i, message in enumerate(messages):
        if verbose:
            print(f"{i + 1} / {len(messages)}")
            print(json.dumps(message))
        if not dryrun:
            print(json.dumps(message))


@click.command()
@click.option(
    "--output",
    "-o",
    default="stdout",
    show_default=True,
    help="""The type of output to use.
    - stdout: print the messages to stdout.
    - file: write the messages to a file. --file/-f must be specified with the filename.
    - kafka: send the messages to a kafka topic. --kafka-host/-k must be specified with the host.
    """,
)
@click.option(
    "--file",
    "-f",
    default="spans.json",
    help="The file to write the messages to.",
)
@click.option(
    "--kafka-host",
    "-k",
    default="127.0.0.1:9092",
    show_default=True,
    help="The host and port for kafka.",
)
@click.option(
    "--kafka-topic",
    "-t",
    default="snuba-spans",
    show_default=True,
    help="The kafka topic to send the messages to.",
)
@click.option(
    "--spans",
    "-s",
    default=None,
    help="Number of spans to generate.",
)
@click.option(
    "--add-datetimes",
    "-ad",
    is_flag=True,
    default=False,
    show_default=True,
    help="Add datetimes to the spans along with timestamps.",
)
@click.option(
    "--dryrun",
    "-d",
    is_flag=True,
    default=False,
    show_default=True,
    help="Generate the messages without sending them.",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    default=False,
    show_default=True,
    help="Enable extra printing when running the script. This will go to stdout.",
)
def main(
    output: str,
    file: str,
    kafka_host: str,
    kafka_topic: str,
    spans: str,
    add_datetimes: bool,
    dryrun: bool,
    verbose: bool,
) -> None:
    to_close = None
    if output == "stdout":
        producer = write_to_stdout
    elif output == "file":
        if not file:
            raise ValueError("--file/-f must be specified when using file output.")
        producer, to_close = create_file_producer(file)  # type: ignore
    elif output == "kafka":
        if not kafka_host:
            raise ValueError(
                "--kafka-host/-k must be specified when using kafka output."
            )
        if not kafka_topic:
            raise ValueError(
                "--kafka-topic/-t must be specified when using kafka output."
            )
        producer, to_close = create_kafka_producer(kafka_host, kafka_topic)  # type: ignore
    else:
        raise ValueError(f"Unknown output type: {output}")

    try:
        messages = create_trace(add_datetimes)
        producer(messages, dryrun, verbose)

        if spans:
            created = len(messages)
            limit = int(spans)

            while created < limit:
                messages = create_trace(add_datetimes)
                producer(messages, dryrun, verbose)
                created += len(messages)
                if verbose:
                    print(f"Created {created} spans.")
    finally:
        if to_close:
            to_close.close()


if __name__ == "__main__":
    main()
