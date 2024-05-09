from datetime import datetime, timedelta

import pytest

from snuba import state
from snuba.state import expiry_window


class TestState:
    def setup_method(self) -> None:
        self.start_test_time = datetime.now()
        self.projects_insert_times_map = {
            0: self.start_test_time,
            1: self.start_test_time,
            2: self.start_test_time + timedelta(minutes=1),
            3: self.start_test_time + timedelta(minutes=1),
            4: self.start_test_time + timedelta(minutes=3),
            5: self.start_test_time + timedelta(minutes=3),
            6: self.start_test_time + timedelta(minutes=4),
            7: self.start_test_time + timedelta(minutes=4),
        }

    @pytest.mark.redis_db
    def test_projects_expire_correctly(self) -> None:
        state.set_config_auto_replacements_bypass_projects(
            [0, 1], self.projects_insert_times_map[1]
        )
        state.set_config_auto_replacements_bypass_projects(
            [2, 3], self.projects_insert_times_map[3]
        )
        state.set_config_auto_replacements_bypass_projects(
            [4, 5], self.projects_insert_times_map[5]
        )
        state.set_config_auto_replacements_bypass_projects(
            [6, 7], self.projects_insert_times_map[7]
        )
        assert set(
            state.get_config_auto_replacements_bypass_projects(
                self.projects_insert_times_map[7]
            )
        ) == set([0, 1, 2, 3, 4, 5, 6, 7])
        assert set(
            state.get_config_auto_replacements_bypass_projects(
                self.projects_insert_times_map[3] + expiry_window + timedelta(minutes=1)
            )
        ) == set([4, 5, 6, 7])
        assert (
            set(
                state.get_config_auto_replacements_bypass_projects(
                    self.projects_insert_times_map[7]
                    + expiry_window
                    + timedelta(minutes=1)
                )
            )
            == set()
        )

    @pytest.mark.redis_db
    def test_existing_projects_expiry_do_not_update_upon_new_prjects(self) -> None:
        state.set_config_auto_replacements_bypass_projects(
            [0, 1], self.projects_insert_times_map[1]
        )
        state.set_config_auto_replacements_bypass_projects(
            [0, 1, 4], self.projects_insert_times_map[4]
        )
        assert set(
            state.get_config_auto_replacements_bypass_projects(
                self.projects_insert_times_map[4] + timedelta(minutes=1)
            )
        ) == set([0, 1, 4])
        assert set(
            state.get_config_auto_replacements_bypass_projects(
                self.projects_insert_times_map[4] + expiry_window - timedelta(minutes=1)
            )
        ) == set([4])
