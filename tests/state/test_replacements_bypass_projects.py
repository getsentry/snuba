from datetime import datetime, timedelta

import pytest
from freezegun import freeze_time

from snuba.replacers.replacements_and_expiry import (
    REPLACEMENTS_EXPIRY_WINDOW_MINUTES_KEY,
    get_config_auto_replacements_bypass_projects,
    get_minutes,
    set_config_auto_replacements_bypass_projects,
)


@freeze_time("2024-5-13 09:00:00")
class TestState:
    start_test_time = datetime.now()
    expiry_window_minutes = get_minutes(REPLACEMENTS_EXPIRY_WINDOW_MINUTES_KEY, 5)
    proj1_add_time = start_test_time
    proj2_add_time = start_test_time + timedelta(minutes=expiry_window_minutes // 2)
    proj1_expiry = proj1_add_time + timedelta(minutes=expiry_window_minutes)
    proj2_expiry = proj2_add_time + timedelta(minutes=expiry_window_minutes)

    @pytest.mark.redis_db
    def test_project_does_not_expire_within_expiry(self) -> None:
        set_config_auto_replacements_bypass_projects([1], self.proj1_add_time)
        assert set(
            get_config_auto_replacements_bypass_projects(
                self.proj1_expiry - timedelta(minutes=1)
            )
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
            get_config_auto_replacements_bypass_projects(
                self.proj1_expiry - timedelta(minutes=1)
            )
        ) == set([1, 2])
        assert set(
            get_config_auto_replacements_bypass_projects(
                self.proj1_expiry + timedelta(minutes=1)
            )
        ) == set([2])

    @pytest.mark.redis_db
    def test_expiry_does_not_update(self) -> None:
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
