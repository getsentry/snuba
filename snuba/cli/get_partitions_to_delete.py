from collections.abc import Sequence
from datetime import datetime

import click

from snuba.clickhouse.escaping import escape_string
from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.datasets.storages.factory import get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.environment import setup_logging, setup_sentry
from snuba.utils.constants import ATTRIBUTE_BUCKETS_EAP_ITEMS
from snuba.utils.hashes import fnv_1a

_ATTR_COLUMNS = {"str": "attributes_string", "float": "attributes_float"}


def build_partition_query(
    *,
    table: str,
    org_id: int,
    project_ids: Sequence[int],
    item_type: int,
    start: datetime,
    end: datetime,
    attr_name: str,
    attr_type: str,
    attr_value: str,
) -> str:
    bucket = fnv_1a(attr_name.encode("utf-8")) % ATTRIBUTE_BUCKETS_EAP_ITEMS
    attr_col = f"{_ATTR_COLUMNS[attr_type]}_{bucket}"
    key = escape_string(attr_name)
    projects = ", ".join(str(p) for p in project_ids)
    start_s = escape_string(start.strftime("%Y-%m-%d %H:%M:%S"))
    end_s = escape_string(end.strftime("%Y-%m-%d %H:%M:%S"))
    # shortcut: eap_items v1 only, add eap_items_2 when reads cut over
    return f"""
SELECT DISTINCT _partition_id
FROM {table}
WHERE organization_id = {org_id}
  AND project_id IN ({projects})
  AND item_type = {item_type}
  AND timestamp >= toDateTime({start_s})
  AND timestamp < toDateTime({end_s})
  AND has({attr_col}, {key})
  AND toString({attr_col}[{key}]) ILIKE {escape_string(attr_value)}
""".strip()


@click.command()
@click.option("--org", "org_id", type=int, required=True)
@click.option("--projects", "project_ids", type=int, multiple=True, required=True)
@click.option("--start", type=click.DateTime(), required=True)
@click.option("--end", type=click.DateTime(), required=True)
@click.option("--item-type", "item_type", type=int, required=True)
@click.option("--attr-name", "attr_name", required=True)
@click.option("--attr-type", "attr_type", type=click.Choice(["str", "float"]), required=True)
@click.option("--attr-value", "attr_value", required=True, help="Matched with ILIKE.")
@click.option("--log-level", help="Logging level to use.")
def get_partitions_to_delete(
    *,
    org_id: int,
    project_ids: Sequence[int],
    start: datetime,
    end: datetime,
    item_type: int,
    attr_name: str,
    attr_type: str,
    attr_value: str,
    log_level: str | None,
) -> None:
    """List eap_items ClickHouse partitions that match a delete filter."""
    setup_logging(log_level)
    setup_sentry()

    storage = get_writable_storage(StorageKey.EAP_ITEMS)
    table = storage.get_table_writer().get_schema().get_table_name()
    sql = build_partition_query(
        table=table,
        org_id=org_id,
        project_ids=project_ids,
        item_type=item_type,
        start=start,
        end=end,
        attr_name=attr_name,
        attr_type=attr_type,
        attr_value=attr_value,
    )
    connection = storage.get_cluster().get_query_connection(ClickhouseClientSettings.QUERY)
    result = connection.execute(sql)
    for row in result.results:
        click.echo(row[0])
