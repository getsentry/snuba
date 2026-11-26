"""Flex Template / local entrypoint for the EAP items Kafka -> BigQuery pipeline."""

import logging

from eap_items_dataflow.pipeline import run

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
