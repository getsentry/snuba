from datetime import datetime, timedelta

import pytest

from snuba.replacers.replacements_utils import (
    EXPIRY_WINDOW,
    get_config_auto_replacements_bypass_projects,
    set_config_auto_replacements_bypass_projects,
)

# from freezegun import freeze_time


class TestState:
    def setup_method(self) -> None:
        self.start_test_time = datetime.now()
        self.proj1_add_time = self.start_test_time
        self.proj2_add_time = self.start_test_time + timedelta(mintues=2)
        self.proj1_expiry = self.proj1_add_time + EXPIRY_WINDOW
        self.proj2_expiry = self.proj2_add_time + EXPIRY_WINDOW

    @pytest.mark.redis_db
    def test_projects_expire_correctly(self) -> None:
        set_config_auto_replacements_bypass_projects(
            [0, 1], self.projects_insert_times_map[1]
        )
        set_config_auto_replacements_bypass_projects(
            [2, 3], self.projects_insert_times_map[3]
        )
        set_config_auto_replacements_bypass_projects(
            [4, 5], self.projects_insert_times_map[5]
        )
        set_config_auto_replacements_bypass_projects(
            [6, 7], self.projects_insert_times_map[7]
        )
        assert set(
            get_config_auto_replacements_bypass_projects(
                self.projects_insert_times_map[7]
            )
        ) == set([0, 1, 2, 3, 4, 5, 6, 7])
        assert set(
            get_config_auto_replacements_bypass_projects(
                self.projects_insert_times_map[3] + EXPIRY_WINDOW + timedelta(minutes=1)
            )
        ) == set([4, 5, 6, 7])
        assert (
            set(
                get_config_auto_replacements_bypass_projects(
                    self.projects_insert_times_map[7]
                    + EXPIRY_WINDOW
                    + timedelta(minutes=1)
                )
            )
            == set()
        )

    @pytest.mark.redis_db
    def test_existing_projects_expiry_do_not_update_upon_new_prjects(self) -> None:
        set_config_auto_replacements_bypass_projects(
            [0, 1], self.projects_insert_times_map[1]
        )
        set_config_auto_replacements_bypass_projects(
            [0, 1, 4], self.projects_insert_times_map[4]
        )
        assert set(
            get_config_auto_replacements_bypass_projects(
                self.projects_insert_times_map[4] + timedelta(minutes=1)
            )
        ) == set([0, 1, 4])
        assert set(
            get_config_auto_replacements_bypass_projects(
                self.projects_insert_times_map[4] + EXPIRY_WINDOW - timedelta(minutes=1)
            )
        ) == set([4])
