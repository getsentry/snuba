import os  # NOQA
import pytest

from snuba.snapshots.postgres_snapshot import PostgresSnapshot

META_FILE = """
{
    "snapshot_id": "50a86ad6-b4b7-11e9-a46f-acde48001122",
    "product": "snuba",
    "transactions": {
        "xmin": 3372750,
        "xmax": 3372754,
        "xip_list": []
    },
    "content": [
        {
            "table": "sentry_groupedmessage",
            "columns": [
                "id", "status"
            ]
        },
        {
            "table": "sentry_groupasignee",
            "columns": [
                "id",
                "project_id"
            ]
        }
    ],
    "start_timestamp": 1564703503.682226
}
"""


class TestPostgresSnapshot:
    def __prepare_directory(self, tmp_path, table_content):
        snapshot_base = tmp_path / "cdc-snapshot"
        snapshot_base.mkdir()
        meta = snapshot_base / "metadata.json"
        meta.write_text(META_FILE)
        tables_dir = tmp_path / "cdc-snapshot" / "tables"
        tables_dir.mkdir()
        groupedmessage = tables_dir / "sentry_groupedmessage.csv"
        groupedmessage.write_text(table_content)
        groupassignee = tables_dir / "sentry_groupasignee"
        groupassignee.write_text(
            """id,project_id
"""
        )
        return snapshot_base

    def test_parse_snapshot(self, tmp_path):
        snapshot_base = self.__prepare_directory(
            tmp_path,
            """id,status
0,1
""",
        )
        snapshot = PostgresSnapshot.load("snuba", snapshot_base)
        descriptor = snapshot.get_descriptor()
        assert descriptor.id == "50a86ad6-b4b7-11e9-a46f-acde48001122"
        assert descriptor.xmax == 3372754
        assert descriptor.xmin == 3372750
        assert descriptor.xip_list == []
        tables = {
            table_config.table: table_config.columns
            for table_config in descriptor.tables
        }
        assert "sentry_groupedmessage" in tables
        assert tables["sentry_groupedmessage"] == ["id", "status"]
        assert "sentry_groupasignee" in tables
        assert tables["sentry_groupasignee"] == ["id", "project_id"]

        with snapshot.get_table_file("sentry_groupedmessage") as table:
            line = next(table)
            assert line == {
                "id": "0",
                "status": "1",
            }

    def test_parse_invalid_snapshot(self, tmp_path):
        snapshot_base = self.__prepare_directory(
            tmp_path,
            """id
0
""",
        )
        with pytest.raises(ValueError, match=".+sentry_groupedmessage.+status.+"):
            snapshot = PostgresSnapshot.load("snuba", snapshot_base)
            with snapshot.get_table_file("sentry_groupedmessage") as table:
                next(table)
