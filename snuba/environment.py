import logging
import os

import sentry_sdk
import structlog
from sentry_sdk.integrations.flask import FlaskIntegration
from sentry_sdk.integrations.gnu_backtrace import GnuBacktraceIntegration
from sentry_sdk.integrations.logging import LoggingIntegration
from sentry_sdk.integrations.redis import RedisIntegration
from sentry_sdk.integrations.threading import ThreadingIntegration
from sentry_sdk.types import Event, Hint
from structlog.processors import JSONRenderer
from structlog.types import EventDict
from structlog_sentry import SentryProcessor

from snuba import settings
from snuba.utils.metrics.util import create_metrics


def add_severity_attribute(
    logger: logging.Logger, method_name: str, event_dict: EventDict
) -> EventDict:
    """
    Set the severity attribute for Google Cloud logging ingestion
    """
    if method_name == "warn":
        method_name = "warning"

    event_dict["severity"] = method_name
    event_dict["level"] = method_name

    return event_dict


def drop_level(logger: logging.Logger, method_name: str, event_dict: EventDict) -> EventDict:
    """
    The "SentryProcessor" requires a `level` field but we're already
    emitting it as `severity` for Google Cloud, so we delete the duplication
    if SentryProcessor is done
    """
    del event_dict["level"]

    return event_dict


def setup_logging(level: str | None = None) -> None:
    if level is None:
        level = settings.LOG_LEVEL

    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format=settings.LOG_FORMAT,
        force=True,
    )

    # google-auth's httplib2 transport emits a spurious WARNING on every token
    # refresh: "httplib2 transport does not support per-request timeout. Set the
    # timeout when constructing the httplib2.Http instance." Since google-auth
    # >= 2.39.0 the transport is always handed a per-request timeout (e.g. when
    # refreshing service-account tokens against the GKE metadata server), and the
    # warning fires regardless of how the Http instance was built -- googleapiclient
    # already constructs it with a construction-time timeout, so there is nothing
    # to "fix" on our side. The message is pure noise (132k+ events on snuba-admin
    # with zero user impact), so drop everything below ERROR for this logger. (SNUBA-8P5)
    logging.getLogger("google_auth_httplib2").setLevel(logging.ERROR)

    structlog.configure(
        cache_logger_on_first_use=True,
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        processors=[
            add_severity_attribute,
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            # Mirror the LoggingIntegration policy on the structlog path: only
            # ERROR and above become Sentry issues, WARNING and below stay as
            # logs/breadcrumbs. (This is structlog-sentry's default, set
            # explicitly so the policy is obvious and robust to upstream changes.)
            SentryProcessor(event_level=logging.ERROR),
            drop_level,
            JSONRenderer(),
        ],
    )


def before_send(event: Event, hint: Hint) -> Event | None:
    """
    Drop transient/operational exceptions that recover on their own and aren't
    actionable as Sentry issues. They are still captured as logs/breadcrumbs.

    - ``AllocationPolicyViolations`` / ``RPCAllocationPolicyException``: expected
      query rejections, not bugs.
    - arroyo ``ChildProcessTerminated``: a multiprocessing worker in a consumer
      was killed by a signal (almost always an OOM-kill of the worker, sometimes
      a native crash). arroyo logs this at ERROR via ``logger.exception`` and
      then re-raises it, so a single worker death surfaces as several ERROR-level
      Sentry issues (e.g. SNUBA-BA7/BA8/BA9/BAD/B1T, grouped by wherever the
      SIGCHLD happened to interrupt the run loop) even though the consumer simply
      shuts down and is restarted by the orchestrator. Because these are
      ERROR-level they are not covered by the WARN->log policy, so filter them by
      type here. The underlying worker death is still observable via logs and
      arroyo's ``sigchld.detected`` metric.
    - redis-cluster's own transient connectivity failure ("... cannot be
      connected. Please provide at least one reachable node: ..."), raised as
      a bare ``RedisClusterException``. It self-heals once the cluster is
      reachable again, matching the transient-message convention already used
      for cluster init in ``snuba/redis.py`` (``KNOWN_TRANSIENT_INIT_FAILURE_MESSAGE``),
      so it isn't actionable as a Sentry issue (e.g. SNUBA-BQA, SNUBA-B6Z).
      ``RedisClusterException`` is also raised by redis-py for unrelated,
      genuinely actionable programming errors (unsupported commands in
      cluster mode, invalid arguments, etc.), so this matches on the specific
      message rather than the exception type.
    """
    if "exc_info" not in hint:
        return event

    _, exc_value, _ = hint["exc_info"]
    if exc_value is None:
        return event

    from arroyo.processing.strategies.run_task_with_multiprocessing import (
        ChildProcessTerminated,
    )
    from redis.exceptions import RedisClusterException

    from snuba.query.allocation_policies import AllocationPolicyViolations
    from snuba.web.rpc.common.exceptions import RPCAllocationPolicyException

    noise_types = (
        RPCAllocationPolicyException,
        AllocationPolicyViolations,
        ChildProcessTerminated,
    )

    redis_cluster_transient_message = "cannot be connected"

    # Walk the exception chain (the exception itself plus its __cause__ /
    # __context__) and drop the event if any link is a known noise type.
    seen: set[int] = set()
    exc: BaseException | None = exc_value
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        if isinstance(exc, noise_types):
            return None  # Don't send to Sentry
        if isinstance(exc, RedisClusterException) and redis_cluster_transient_message in str(exc):
            return None  # Don't send to Sentry
        # Follow the chain the way Python itself displays it: an explicit cause
        # (`raise ... from other`) wins, otherwise the implicit context -- unless
        # it was suppressed via `raise ... from None`, in which case we must not
        # follow __context__ or we could drop an unrelated event whose suppressed
        # context happens to contain a noise type.
        if exc.__cause__ is not None:
            exc = exc.__cause__
        elif not exc.__suppress_context__:
            exc = exc.__context__
        else:
            exc = None

    return event


def setup_sentry() -> None:
    sentry_sdk.init(
        dsn=settings.SENTRY_DSN,
        spotlight=None if settings.DEBUG else False,
        before_send=before_send,
        integrations=[
            FlaskIntegration(),
            GnuBacktraceIntegration(),
            # Only forward ERROR and above to Sentry as issues. Warnings are
            # operational noise far more often than they're actionable (e.g.
            # consumer rebalance/shutdown timeouts) and are still captured as
            # logs/breadcrumbs.
            LoggingIntegration(event_level=logging.ERROR),
            RedisIntegration(),
            ThreadingIntegration(propagate_hub=True),
        ],
        # the value for release is also computed in rust-snuba, please keep the
        # logic in sync
        release=os.getenv("SNUBA_RELEASE"),
        traces_sample_rate=settings.SENTRY_TRACE_SAMPLE_RATE,
        profiles_sample_rate=settings.SNUBA_PROFILES_SAMPLE_RATE,
        _experiments={
            # Turns on the metrics module
            "enable_metrics": True,
            # Enables sending of code locations for metrics
            "metric_code_locations": True,
        },
    )

    from snuba.state.sentry_options import init_options

    init_options()

    from snuba.utils.profiler import run_ondemand_profiler

    if settings.SENTRY_DSN is not None:
        # Do not run ondemand profiler in tests, it interferes with mocked
        # `time.sleep()` and assertions on that mock.
        run_ondemand_profiler()


metrics = create_metrics(
    "snuba",
    tags=None,
    sample_rates=settings.DOGSTATSD_SAMPLING_RATES,
)
