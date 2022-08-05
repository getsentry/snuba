from snuba.datasets.storage import WritableTableStorage


def deep_compare_storages(old: WritableTableStorage, new: WritableTableStorage) -> None:
    assert (
        old.get_cluster().get_clickhouse_cluster_name()
        == new.get_cluster().get_clickhouse_cluster_name()
    )
    assert (
        old.get_cluster().get_storage_set_keys()
        == new.get_cluster().get_storage_set_keys()
    )
    assert old.get_is_write_error_ignorable() == new.get_is_write_error_ignorable()
    assert (
        old.get_mandatory_condition_checkers() == new.get_mandatory_condition_checkers()
    )
    assert old.get_query_processors() == new.get_query_processors()
    assert old.get_query_splitters() == new.get_query_splitters()
    assert (
        old.get_schema().get_columns().columns == new.get_schema().get_columns().columns
    )
