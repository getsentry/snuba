from pathlib import Path

import pytest

from snuba.snapshots import (  # NOQA
    ColumnConfig,
    DateFormatPrecision,
    DateTimeFormatterConfig,
)
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
            "zip": false,
            "columns": [
                {"name": "id"},
                {"name": "status"}
            ]
        },
        {
            "table": "sentry_groupasignee",
            "zip": true,
            "columns": [
                {"name": "id"},
                {
                    "name": "a_date",
                    "formatter": {
                        "type": "datetime",
                        "precision": "second"
                    }
                }
            ]
        }
    ],
    "start_timestamp": 1564703503.682226
}
"""


class TestPostgresSnapshot:
    def __prepare_directory(self, tmp_path: Path, table_content: str) -> str:
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
        return str(snapshot_base)

    def test_parse_snapshot(self, tmp_path: Path) -> None:
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
            table_config.table: (table_config.columns, table_config.zip)
            for table_config in descriptor.tables
        }
        assert "sentry_groupedmessage" in tables
        assert tables["sentry_groupedmessage"] == (
            [ColumnConfig("id"), ColumnConfig("status")],
            False,
        )
        assert "sentry_groupasignee" in tables
        assert tables["sentry_groupasignee"] == (
            [
                ColumnConfig("id"),
                ColumnConfig(
                    "a_date",
                    formatter=DateTimeFormatterConfig(
                        precision=DateFormatPrecision.SECOND
                    ),
                ),
            ],
            True,
        )

        with snapshot.get_parsed_table_file("sentry_groupedmessage") as table:
            assert next(table) == {
                "id": "0",
                "status": "1",
            }

        with snapshot.get_preprocessed_table_file("sentry_groupedmessage") as table:
            assert next(table) == b"id,status\n0,1\n"

    def test_parse_invalid_snapshot(self, tmp_path: Path) -> None:
        snapshot_base = self.__prepare_directory(
            tmp_path,
            """id
0
""",
        )
        with pytest.raises(ValueError, match=".+sentry_groupedmessage.+status.+"):
            snapshot = PostgresSnapshot.load("snuba", snapshot_base)
            with snapshot.get_parsed_table_file("sentry_groupedmessage") as table:
                next(table)
