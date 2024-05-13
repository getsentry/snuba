from datetime import datetime, timedelta

import pytest

from snuba.replacers.replacements_utils import (
    EXPIRY_WINDOW,
    get_config_auto_replacements_bypass_projects,
    set_config_auto_replacements_bypass_projects,
)


class TestState:
    def setup_method(self) -> None:
        self.start_test_time = datetime.now()
        self.proj1_add_time = self.start_test_time
        self.proj2_add_time = self.start_test_time + timedelta(minutes=2)
        self.proj1_expiry = self.proj1_add_time + EXPIRY_WINDOW
        self.proj2_expiry = self.proj2_add_time + EXPIRY_WINDOW

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
            [1], self.proj1_add_time + timedelta(minutes=2)
        )
        assert (
            set(
                get_config_auto_replacements_bypass_projects(
                    self.proj1_expiry + timedelta(minutes=1)
                )
            )
            == set()
        )
