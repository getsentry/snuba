import pytest

from snuba.clickhouse import DATETIME_FORMAT
from snuba.datasets.cdc.groupedmessage_processor import GroupedMessageRow


class TestFastGroupedMessageLoad:
    def test_supported_date_format(self) -> None:
        """
        This test is to ensure the compatibility between
        the clickhouse datetime format and the hardcoded
        one in the bulk load process (for performance).
        If someone changes the format without updating the
        bulk loader this test will fail.
        """
        assert DATETIME_FORMAT == "%Y-%m-%d %H:%M:%S"

    def test_basic_date(self) -> None:
        message = GroupedMessageRow.from_bulk(
            {
                "project_id": "2",
                "id": "1",
                "status": "0",
                # UTC date with nanoseconds
                "last_seen": "2019-07-01 18:03:07.984123+00",
                # UTC date without nanosecods
                "first_seen": "2019-07-01 18:03:07+00",
                # None date
                "active_at": "",
                "first_release_id": "0",
            }
        )

        assert message.to_clickhouse() == {
            "offset": 0,
            "project_id": 2,
            "id": 1,
            "record_deleted": 0,
            "status": 0,
            "last_seen": "2019-07-01 18:03:07",
            "first_seen": "2019-07-01 18:03:07",
            "active_at": None,
            "first_release_id": 0,
        }

    def test_failure(self) -> None:
        with pytest.raises(AssertionError):
            GroupedMessageRow.from_bulk(
                {
                    "project_id": "2",
                    "id": "1",
                    "status": "0",
                    # Non UTC date with nanoseconds
                    "last_seen": "2019-07-01 18:03:07.984+05",
                    # UTC date without nanosecods
                    "first_seen": "2019-07-01 18:03:07+00",
                    # another UTC date with less precision
                    "active_at": "2019-06-25 22:15:57.6+00",
                    "first_release_id": "0",
                }
            )
