# pylint: skip-file
# flake8: noqa

import datetime
import json
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


DeleteMessage = dict[str, Any]
Producer = Callable[[list[DeleteMessage], bool, bool], None]

STATUS_CODES = ["200", "300", "400"]
OPS = ["db", "cache", "http"]

COLUMN_VALUES = [
    [1, [2, 3]],
    [1, [4, 5]],
    [2, [6, 7]],
    [3, [8, 9]],
    [4, [1, 2]],
    [4, [9, 10, 11]],
]


def create_delete(column_values: list[Any]) -> DeleteMessage:
    return {
        "storage_name": "search_issues",
        "columns": ["project_id", "group_id"],
        "column_values": column_values,
        "received": RECEIVED,
        "num_rows": 1,
    }


def create_kafka_producer(host: str, topic: str) -> tuple[Producer, Any]:
    from arroyo.backends.kafka import KafkaPayload, KafkaProducer
    from arroyo.types import Topic

    conf = {"bootstrap.servers": host}

    kafka_producer = KafkaProducer(conf)

    def producer(messages: list[DeleteMessage], dryrun: bool, verbose: bool) -> None:
        for i, message in enumerate(messages):
            if verbose:
                print(f"{i + 1} / {len(messages)}")
                print(json.dumps(message))
            if not dryrun:
                kafka_producer.produce(
                    Topic(name=(topic)),
                    KafkaPayload(
                        key=None, value=json.dumps(message).encode("utf-8"), headers=[]
                    ),
                )

    return producer, kafka_producer


def create_file_producer(filename: str) -> tuple[Producer, Any]:
    file_descriptor = open(filename, "a")

    def producer(messages: list[DeleteMessage], dryrun: bool, verbose: bool) -> None:
        for i, message in enumerate(messages):
            if verbose:
                print(f"{i + 1} / {len(messages)}")
                print(json.dumps(message))
            if not dryrun:
                file_descriptor.write(json.dumps(message))
                file_descriptor.write("\n")

    return producer, file_descriptor


def write_to_stdout(messages: list[DeleteMessage], dryrun: bool, verbose: bool) -> None:
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
    default="deletes.json",
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
    default="snuba-lw-deletions",
    show_default=True,
    help="The kafka topic to send the messages to.",
)
@click.option(
    "--deletes",
    "-d",
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
    deletes: str,
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

        messages = [create_delete(cv) for cv in COLUMN_VALUES]
        producer(messages, dryrun, verbose)

        # if deletes:
        #     created = len(messages)
        #     limit = int(deletes)

        #     while created < limit:
        #         messages = create_delete(add_datetimes)
        #         producer(messages, dryrun, verbose)
        #         created += len(messages)
        #         if verbose:
        #             print(f"Created {created} deletes.")
    finally:
        if to_close:
            to_close.close()


if __name__ == "__main__":
    main()
