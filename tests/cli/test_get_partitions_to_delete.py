from datetime import datetime

from snuba.cli.get_partitions_to_delete import build_partition_query
from snuba.utils.constants import ATTRIBUTE_BUCKETS_EAP_ITEMS
from snuba.utils.hashes import fnv_1a


def test_build_partition_query_includes_pkey_and_string_ilike() -> None:
    attr_name = "sentry.description"
    bucket = fnv_1a(attr_name.encode("utf-8")) % ATTRIBUTE_BUCKETS_EAP_ITEMS
    sql = build_partition_query(
        table="eap_items_1_dist",
        org_id=1,
        project_ids=(2, 3),
        item_type=4,
        start=datetime(2024, 1, 1),
        end=datetime(2024, 1, 8),
        attr_name=attr_name,
        attr_type="str",
        attr_value="%foo%",
    )
    assert "FROM eap_items_1_dist" in sql
    assert "organization_id = 1" in sql
    assert "project_id IN (2, 3)" in sql
    assert "item_type = 4" in sql
    assert "timestamp >= toDateTime('2024-01-01 00:00:00')" in sql
    assert "timestamp < toDateTime('2024-01-08 00:00:00')" in sql
    assert f"attributes_string_{bucket}" in sql
    assert "ILIKE '%foo%'" in sql


def test_build_partition_query_float_uses_float_bucket() -> None:
    attr_name = "sentry.duration"
    bucket = fnv_1a(attr_name.encode("utf-8")) % ATTRIBUTE_BUCKETS_EAP_ITEMS
    sql = build_partition_query(
        table="eap_items_1_dist",
        org_id=1,
        project_ids=(2,),
        item_type=1,
        start=datetime(2024, 1, 1),
        end=datetime(2024, 1, 2),
        attr_name=attr_name,
        attr_type="float",
        attr_value="1%",
    )
    assert f"attributes_float_{bucket}" in sql
    assert "attributes_string_" not in sql
