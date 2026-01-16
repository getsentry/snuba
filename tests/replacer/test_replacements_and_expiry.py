import typing
from datetime import datetime, timedelta
from unittest import mock

import pytest
from freezegun import freeze_time

from snuba.replacers.replacements_and_expiry import (
    REPLACEMENTS_EXPIRY_WINDOW_MINUTES_KEY,
    get_config_auto_replacements_bypass_projects,
    set_config_auto_replacements_bypass_projects,
)
from snuba.state import get_int_config


@freeze_time("2024-5-13 09:00:00")
class TestState:
    start_test_time = datetime.now()
    expiry_window_minutes = typing.cast(
        int, get_int_config(REPLACEMENTS_EXPIRY_WINDOW_MINUTES_KEY, 5)
    )
    proj1_add_time = start_test_time
    proj2_add_time = start_test_time + timedelta(minutes=expiry_window_minutes // 2)
    proj1_expiry = proj1_add_time + timedelta(minutes=expiry_window_minutes)
    proj2_expiry = proj2_add_time + timedelta(minutes=expiry_window_minutes)

    @pytest.mark.redis_db
    def test_project_does_not_expire_within_expiry(self) -> None:
        set_config_auto_replacements_bypass_projects([1], self.proj1_add_time)
        assert set(
            get_config_auto_replacements_bypass_projects(self.proj1_expiry - timedelta(minutes=1))
        ) == set([1])

    @pytest.mark.redis_db
    def test_project_expires_after_expiry(self) -> None:
        set_config_auto_replacements_bypass_projects([1], self.proj1_add_time)
        assert (
            set(
                get_config_auto_replacements_bypass_projects(
                    self.proj1_expiry + timedelta(minutes=1)
                )
            )
            == set()
        )

    @pytest.mark.redis_db
    def test_multiple_projects(self) -> None:
        set_config_auto_replacements_bypass_projects([1], self.proj1_add_time)
        set_config_auto_replacements_bypass_projects([2], self.proj2_add_time)
        assert set(
            get_config_auto_replacements_bypass_projects(self.proj1_expiry - timedelta(minutes=1))
        ) == set([1, 2])
        assert set(
            get_config_auto_replacements_bypass_projects(self.proj1_expiry + timedelta(minutes=1))
        ) == set([2])

    @pytest.mark.redis_db
    def test_expiry_does_not_update(self) -> None:
        assert self.expiry_window_minutes is not None
        set_config_auto_replacements_bypass_projects([1], self.proj1_add_time)
        set_config_auto_replacements_bypass_projects(
            [1],
            self.proj1_add_time + timedelta(minutes=self.expiry_window_minutes // 2),
        )
        assert (
            set(
                get_config_auto_replacements_bypass_projects(
                    self.proj1_expiry + timedelta(minutes=1)
                )
            )
            == set()
        )

    @pytest.mark.redis_db
    @mock.patch(
        "snuba.replacers.replacements_and_expiry.get_int_config",
    )
    def test_expiry_window_changes(self, mock: mock.MagicMock) -> None:
        mock.side_effect = [5, 10]
        set_config_auto_replacements_bypass_projects([1], self.proj1_add_time)
        set_config_auto_replacements_bypass_projects([2], self.proj2_add_time)
        # project 1 expires after 5 minutes
        assert set(
            get_config_auto_replacements_bypass_projects(self.proj1_add_time + timedelta(minutes=6))
        ) == set([2])
        # project 2 expires at 10 minutes
        assert set(
            get_config_auto_replacements_bypass_projects(self.proj2_add_time + timedelta(minutes=9))
        ) == set([2])
        assert set(
            get_config_auto_replacements_bypass_projects(
                self.proj2_add_time + timedelta(minutes=11)
            )
        ) == set([])
