from tests.base import BaseDatasetTest

from snuba.datasets.factory import DATASET_NAMES, get_dataset


class TestMigrate(BaseDatasetTest):
    def setup_method(self, test_method):
        # Create every table
        for dataset_name in DATASET_NAMES:
            super().setup_method(test_method, dataset_name)

    def teardown_method(self, test_method):
        for dataset_name in DATASET_NAMES:
            super().teardown_method(test_method)

    def test_runs_migrations_without_errors(self):
        from snuba.migrations.migrate import run

        for dataset_name in DATASET_NAMES:
            dataset = get_dataset(dataset_name)
            run(self.clickhouse, dataset)

    def test_no_schema_diffs(self):
        from snuba.migrations.parse_schema import get_local_schema

        for dataset_name in DATASET_NAMES:
            table_writer = get_dataset(dataset_name).get_table_writer()
            if not table_writer:
                continue

            dataset_schema = table_writer.get_schema()
            local_table_name = dataset_schema.get_local_table_name()
            local_schema = get_local_schema(self.clickhouse, local_table_name)

            assert not dataset_schema.get_column_differences(local_schema)
