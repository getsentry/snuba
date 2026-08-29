===============
Snuba Consumers
===============

Summary
-------

* ``snuba consumer`` is still the easiest way to deploy a consumer for a new
  dataset, as the message processor can be written in Python.

* ``snuba rust-consumer --use-rust-processor`` can be used to serve the needs
  of high-throughput datasets, as it sidesteps all problems with Python
  concurrency. For that, the message processor needs to be written to Rust.

* ``snuba rust-consumer --use-python-processor`` is an experiment that attempts
  to consolidate both consumers' logic, so that we can eventually remove the
  code beneath ``snuba consumer``. It is not deployed anywhere in prod.

* In order to port a dataset fully to Rust, register a new message processor in
  ``rust_snuba/src/processors/mod.rs`` and add it to
  ``tests/consumers/test_message_processors.py``. Later, use
  ``RustCompatProcessor`` to get rid of the Python implementation.

Message processors
------------------

Each storage in Snuba defines a conversion function mapping the layout of a
Kafka message to a list of ClickHouse rows.

Message processors are defined in :py:mod:`snuba.dataset.processors`, and
need to subclass from
:py:class:`snuba.dataset.processors.DatasetMessageProcessor`. Just by
subclassing, their name becomes available for reference in
``snuba/datasets/configuration/*/storages/*.yaml``.

Python consumers
----------------

``snuba consumer`` runs the Python consumer. It uses the Python message
processor mentioned above to convert Kafka messages into rows, batches those
rows up into larger ``INSERT``-statements, sends off the ``INSERT`` statement
to the cluster defined in ``settings.py``.

Test endpoints
--------------

In sentry we have a lot of tests that want to insert into ClickHouse. Tests
have certain requirements that our Kafka consumers can't meet and which don't
apply to production:


* They require strong consistency, as they want to run a handful of queries +
  assertions, then insert a few rows, wait for them to be inserted, then run
  some more assertions depending on that new data.

* Because tests wait for every insert synchronously, insertion latency is
  really important, while throughput isn't.

* Basically, people want to write e2e tests involving Snuba similarly to how
  tests involving relational DBs in Django ORM are being written.

Every storage can be inserted into and wiped using HTTP as well, using the
endpoints defined in ``snuba.web.views`` prefixed with ``/tests/``. Those
endpoints use the same message processors as the Python consumer, but there's
no batching at all. One HTTP request gets directly translated into a blocking
``INSERT``-statement towards ClickHouse.

Rust consumers
--------------

``snuba rust-consumer`` runs the Rust consumer. It comes in two flavors, "pure
Rust" and "hybrid":

* ``--use-rust-processor`` ("pure rust") will attempt to find and load a Rust
  version of the message processor. There is a mapping from Python class names
  like ``QuerylogProcessor`` to the relevant function, defined in
  ``rust_snuba/src/processors/mod.rs``. If that function exists, it is being
  used. The resulting running consumer is sometimes 20x faster than the Python
  version.

  If a Rust port of the message processor can't be found, the consumer silently
  falls back to the second flavor:

* ``--use-python-processor`` ("hybrid") will use the Python message processor from
  within Rust. For this mode, no dataset-specific logic has to be ported to
  Rust, but at the same time the performance benefits of using a Rust consumer
  are negligible.

Python message processor compat shims
-------------------------------------

Even when a dataset is being processed with 100% Rust in prod (i.e. pure-rust
consumer is being used), we still have those ``/tests/`` API endpoints, as
there is a need for testing, and so there still needs to be a Python
implementation of the same message processor. For this purpose
``RustCompatProcessor`` can be used as a baseclass that will delegate all logic
back into Rust. This means that:

* ``snuba consumer`` will connect to Kafka using Python, but process messages
  in Rust (using Python's multiprocessing)
* ``snuba rust-consumer --use-python-processor`` makes no sense to deploy
  anywhere, as it will connect to Kafka using Rust, then perform a roundtrip
  through Python only to call Rust business logic again.

Testing
-------

When porting a message processor to Rust, we validate equivalence to Python by:

1. Registering the Python class in
   ``tests/consumers/test_message_processors.py``, where it will run all
   payloads from ``sentry-kafka-schemas`` against both message processors and
   assert the same rows come back. If there is missing test coverage, it's
   preferred to add more payloads to ``sentry-kafka-schemas`` than to write
   custom tests.

2. Remove the Python message processor and replace it with
   ``RustCompatProcessor``, in which case the existing Python tests (of both
   Snuba and Sentry) will be directly run against the Rust message processor.

Architecture
------------

Python
~~~~~~

In order to get around the GIL, Python consumers use arroyo's multiprocessing
support to be able to use multiple cores. This comes with significant
serialization overhead and an amount of complexity that is out of scope for
this document.

Pure Rust
~~~~~~~~~

Despite the name this consumer is still launched from the Python CLI. The way
this works is that all Rust is compiled into a shared library exposing a
``consumer()`` function, and packaged using ``maturin`` into a Python wheel.
The Python CLI for ``snuba rust-consumer``:

1. Parses CLI arguments
2. Resolves config (loads storages, clickhouse settings)
3. Builds a new config JSON payload containing only information relevant to the
   Rust consumer (name of message processor, name of physical Kafka topic, name
   of ClickHouse table, and connection settings)
4. calls ``rust_snuba.consumer(config)``, at which point Rust takes over the
   process entirely.

Concurrency model in pure-rust is very simple: The message processors run on a
``tokio::Runtime``, which means that we're using regular OS threads in order to
use multiple cores. The GIL is irrelevant since no Python code runs.

Hybrid
~~~~~~

Hybrid consumer is mostly the same as pure-rust. The main difference is that it
calls back into Python message processors. How that works is work-in-progress,
but fundamentally it is subject to the same concurrency problems as the regular
pure-Python consumer, and is therefore forced to spawn subprocesses and perform
IPC one way or the other.

Since the consumer is launched from the Python CLI, it will find the Python
interpreter already initialized, and does not have to re-import Snuba again
(except in subprocesses)



Healthchecks
------------

Kafka consumers can look alive to the broker while making no useful progress
(or while only one assigned partition is stuck). Snuba uses a health *file*
plus Kubernetes probes so those pods get restarted and Kafka rebalances.

Strategy implementations
~~~~~~~~~~~~~~~~~~~~~~~~

Selected with ``--health-check`` on ``snuba rust-consumer`` (and accepted
outcomes). Requires ``--health-check-file``; without the file path there is no
pod-level check.

==================  =========  =========================================================
Value               Default    Behavior
==================  =========  =========================================================
``arroyo``          yes        Stock arroyo strategy: touch the health file on every
                               successful ``poll``. Catches a blocked main loop only.
``snuba``           no         Snuba strategy in ``rust_snuba``. Same file touch, plus
                               optional progress modes below. Also selected automatically
                               when ``consumer.partition_stall_timeout_secs > 0``.
==================  =========  =========================================================

Python consumers only support the arroyo-style file touch (no ``--health-check``
switch).

Kubernetes probes
~~~~~~~~~~~~~~~~~

Startup and liveness probes remove the health file and expect the consumer to
recreate it. Probe period is independent of the strategy. If the strategy stops
touching the file, the pod is killed and the process leaves the consumer group.

Snuba progress modes (sentry-options)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

These only apply when the **snuba** strategy is active.

.. list-table::
   :header-rows: 1
   :widths: 28 18 54

   * - Mode
     - Option
     - Healthy when / what it catches
   * - Default snuba
     - (options off)
     - Every successful ``poll`` (same as arroyo). Catches main thread not polling.
   * - Commit progress
     - ``consumer.commit_progress_healthcheck``
     - A commit request is observed, or the consumer is idle (no recent submits).
       Unhealthy while work is in flight without commits. **Consumer-level**, not
       per-partition: one stuck partition can stay healthy if siblings still commit.
   * - Hard stall
     - ``consumer.partition_stall_timeout_secs > 0``
     - Every partition with in-flight work has committed within the timeout.
       Catches a partition that stopped committing while still receiving work.
   * - Relative slowdown
     - same timeout + ``consumer.partition_slow_ratio``
       (default ``0.25``; set ``0`` for hard-stall only)
     - No active partition's commit rate is below ``ratio × leave-one-out median sibling rate``
       on this assignment (the candidate partition is excluded from the median).
       Catches single-partition throughput collapse where offsets still move slowly.
       Needs ≥2 active partitions on the pod. Quiet assignments are skipped when the
       inclusive assignment median is under 50 offsets/s, so one hotter peer cannot
       force mostly-quiet partitions into the check.


Enable examples (sentry-options)::

    # consumer-level: fail if busy but not committing
    consumer.commit_progress_healthcheck: true

    # per-partition: restart on stall or severe relative slowdown
    consumer.partition_stall_timeout_secs: 300
    consumer.partition_slow_ratio: 0.25

``snuba health`` (API / ClickHouse readiness) is unrelated to this Kafka
consumer file healthcheck.

Static membership (KIP-345)
---------------------------

Both ``snuba consumer`` and ``snuba rust-consumer`` accept
``--group-instance-id``. When set, Snuba passes ``group.instance.id`` into the
librdkafka consumer config. Kafka then treats the process as a **static
member**: after a restart it rejoins with the same id and keeps the same
partitions as long as it returns within ``session.timeout.ms`` (no stop-the-world
rebalance for that bounce).

This is the application-side half of multi-Deployment (or StatefulSet) rollouts
where each pod has a fixed identity, e.g. ops ``static_membership`` for
``eap-items-consumer`` (one single-replica Deployment per member, Recreate
strategy, ``--group-instance-id snuba-eap-items-consumer-<idx>``).

Requirements / caveats:

* **Uniqueness.** Two live members of the same consumer group must never share
  an id (broker returns ``FENCED_INSTANCE_ID``). Deployments that keep a fixed
  id must use ``Recreate`` (or equivalent terminate-before-create), not a rolling
  surge.
* **session.timeout.ms.** Controls how long a member can be gone before its
  partitions are reassigned. Snuba does not expose a dedicated CLI flag; set it
  via Kafka broker/topic consumer config (and keep
  ``max.poll.interval.ms >= session.timeout.ms``). ``--max-poll-interval-ms`` on
  the CLI only lowers session timeout when it is under 45s (librdkafka default).
* **Concurrency.** ``--concurrency`` on ``rust-consumer`` is in-process worker
  threads for message processing, not multiple Kafka members. One process still
  equals one ``group.instance.id``.
* **Observability.** When static membership is enabled the Rust consumer logs
  the resolved ``group.instance.id`` at startup.


Signal-handling is a bit tricky. Since no Python code runs for the majority of
the consumer's lifetime, Python's signal handlers cannot run. This also means
that the Rust consumer has to register its own handler for ``Ctrl-C``, but
doing so also means that Python's own signal handlers are completely ignored.
This is fine for the pure-rust case, but in the Hybrid case we have some Python
code still running. For that Python code, ``KeyboardInterrupt`` does
not work.
