from concurrent.futures import ProcessPoolExecutor
from logging import getLogger

from snuba.cli.rust_consumer import rust_consumer_impl

NUM_CONSUMERS = 4

logger = getLogger(__name__)

if __name__ == "__main__":
    with ProcessPoolExecutor(max_workers=NUM_CONSUMERS) as executor:
        for i in range(NUM_CONSUMERS):
            logger.info(f"starting consumer {i}")
            executor.submit(
                rust_consumer_impl,
                ("eap_items",),
                "eap_items_group",
                no_strict_offset_reset=True,
                auto_offset_reset="latest",
                enforce_schema=True,
            )
