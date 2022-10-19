from snuba.migrations.groups import (
    OPTIONAL_GROUPS,
    REGISTERED_GROUPS_LOOKUP,
    MigrationGroup,
    get_group_loader,
)
from snuba.migrations.migration import Migration
from tests.datasets.configuration.utils import ConfigurationTest


class TestMigrationGroupConfiguration(ConfigurationTest):
    def test_generic_metrics_configuration(self) -> None:
        assert "generic_metrics" in REGISTERED_GROUPS_LOOKUP
        assert "generic_metrics" in OPTIONAL_GROUPS

        generic_metrics_loader = get_group_loader(MigrationGroup("generic_metrics"))

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
