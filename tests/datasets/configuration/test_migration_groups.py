from snuba.migrations.groups import (
    OPTIONAL_GROUPS,
    REGISTERED_GROUPS,
    MigrationGroup,
    get_group_loader,
)
from snuba.migrations.migration import Migration


def test_generic_metrics_configuration() -> None:
    assert MigrationGroup.GENERIC_METRICS in REGISTERED_GROUPS
    assert MigrationGroup.GENERIC_METRICS in OPTIONAL_GROUPS

    generic_metrics_loader = get_group_loader(MigrationGroup.GENERIC_METRICS)

    assert generic_metrics_loader.get_migrations() == [
        "0001_sets_aggregate_table",
        "0002_sets_raw_table",
        "0003_sets_mv",
        "0004_sets_raw_add_granularities",
        "0005_sets_replace_mv",
        "0006_sets_raw_add_granularities_dist_table",
        "0007_distributions_aggregate_table",
        "0008_distributions_raw_table",
        "0009_distributions_mv",
    ]

    m = generic_metrics_loader.load_migration("0005_sets_replace_mv")
    assert isinstance(m, Migration)
