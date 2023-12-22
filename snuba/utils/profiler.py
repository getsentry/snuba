import logging
import socket
import threading
import time

import sentry_sdk
from sentry_sdk.tracing import Transaction

from snuba.state import get_config

logger = logging.getLogger(__name__)


def run_ondemand_profiler() -> None:
    thread = threading.Thread(
        target=_profiler_main, name="snuba ondemand profiler", daemon=True
    )
    thread.start()


def _profiler_main() -> None:
    current_transaction = None
    own_hostname = socket.gethostname()

    while True:
        queried_hostnames = get_config("ondemand_profiler_hostnames") or ""
        queried_hostnames = queried_hostnames.split(",")

        if own_hostname in queried_hostnames and current_transaction is None:
            # Log an error to Sentry on purpose, if the pod slows down it
            # should be obvious why.
            logger.warn("starting ondemand profile for %s", own_hostname)

            with sentry_sdk.Hub.main:
                open_transaction = sentry_sdk.start_transaction(
                    name=f"ondemand profile: {own_hostname}", sampled=True
                )
                assert isinstance(open_transaction, Transaction)
                assert open_transaction._profile is not None
                open_transaction._profile.sampled = True
                if open_transaction._profile.scheduler is None:
                    logger.warn(
                        "unable to start ondemand profile, need to turn on profiling globally"
                    )
                    return

                # Set main thread as active thread -- this current thread is
                # fairly uninteresting to look at.
                # The profile contains all threads anyway.
                open_transaction._profile.active_thread_id = (
                    threading.main_thread().ident
                )

                open_transaction.__enter__()
                transaction_start = time.time()
                current_transaction = open_transaction, transaction_start

            continue

        if current_transaction is not None:
            open_transaction, transaction_start = current_transaction
            if (
                own_hostname not in queried_hostnames
                or time.time() - transaction_start >= 30
            ):
                logger.warn("stopping ondemand profile for %s", own_hostname)
                with sentry_sdk.Hub.main:
                    open_transaction.__exit__(None, None, None)
                    open_transaction = None

            continue

        time.sleep(5)
