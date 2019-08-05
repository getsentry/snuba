import os  # NOQA

from snuba.snapshots.postgres_snapshot import PostgresSnapshot


class TestPostgresSnapshot:

    def test_parse_snapshot(self, tmp_path):
        snapshot_base = tmp_path / "cdc-snapshot"
        snapshot_base.mkdir()
        meta = snapshot_base / "metadata.json"
        meta.write_text("""
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
        """)
        tables_dir = tmp_path / "cdc-snapshot" / "tables"
        tables_dir.mkdir()
        groupedmessage = tables_dir / "sentry_groupedmessage.csv"
        groupedmessage.write_text(
            """id, status
0, 1
"""
        )
        groupassignee = tables_dir / "sentry_groupasignee"
        groupassignee.write_text(
            """id, project_id
"""
        )

        snapshot = PostgresSnapshot.load(snapshot_base, "snuba")
        descriptor = snapshot.get_descriptor()
        assert descriptor.id == "50a86ad6-b4b7-11e9-a46f-acde48001122"
        assert descriptor.xmax == 3372754
        assert descriptor.xmin == 3372750
        assert descriptor.xip_list == []
        tables = {
            table_config.table: table_config.columns for table_config in descriptor.tables
        }
        assert "sentry_groupedmessage" in tables
        assert tables["sentry_groupedmessage"] == ["id", "status"]
        assert "sentry_groupasignee" in tables
        assert tables["sentry_groupasignee"] == ["id", "project_id"]

        with snapshot.get_table_file("sentry_groupedmessage") as table:
            content = table.readlines()
            assert content[0] == "id, status\n"
            assert content[1] == "0, 1\n"
