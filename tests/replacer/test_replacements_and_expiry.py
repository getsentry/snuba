import time
from collections.abc import Iterator
from datetime import datetime, timedelta
from unittest import mock

import pytest
from freezegun import freeze_time

from snuba.replacers import replacements_and_expiry
from snuba.replacers.replacements_and_expiry import (
    AUTO_REPLACEMENTS_BYPASS_CACHE_TTL_KEY,
    REPLACEMENTS_EXPIRY_WINDOW_MINUTES_KEY,
    get_auto_replacements_bypass_projects_cached,
    get_config_auto_replacements_bypass_projects,
    set_config_auto_replacements_bypass_projects,
)
from snuba.state.sentry_options import get_option


@freeze_time("2024-5-13 09:00:00")
class TestState:
    start_test_time = datetime.now()
    expiry_window_minutes = get_option(REPLACEMENTS_EXPIRY_WINDOW_MINUTES_KEY, 5)
    proj1_add_time = start_test_time
    proj2_add_time = start_test_time + timedelta(minutes=expiry_window_minutes // 2)
    proj1_expiry = proj1_add_time + timedelta(minutes=expiry_window_minutes)
    proj2_expiry = proj2_add_time + timedelta(minutes=expiry_window_minutes)

    @pytest.mark.redis_db
    def test_project_does_not_expire_within_expiry(self) -> None:
        set_config_auto_replacements_bypass_projects([1], self.proj1_add_time)
        assert set(
            get_config_auto_replacements_bypass_projects(self.proj1_expiry - timedelta(minutes=1))
        ) == {1}

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
        ) == {1, 2}
        assert set(
            get_config_auto_replacements_bypass_projects(self.proj1_expiry + timedelta(minutes=1))
        ) == {2}

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
        "snuba.replacers.replacements_and_expiry.get_option",
    )
    def test_expiry_window_changes(self, mock: mock.MagicMock) -> None:
        mock.side_effect = [5, 10]
        set_config_auto_replacements_bypass_projects([1], self.proj1_add_time)
        set_config_auto_replacements_bypass_projects([2], self.proj2_add_time)
        # project 1 expires after 5 minutes
        assert set(
            get_config_auto_replacements_bypass_projects(self.proj1_add_time + timedelta(minutes=6))
        ) == {2}
        # project 2 expires at 10 minutes
        assert set(
            get_config_auto_replacements_bypass_projects(self.proj2_add_time + timedelta(minutes=9))
        ) == {2}
        assert (
            set(
                get_config_auto_replacements_bypass_projects(
                    self.proj2_add_time + timedelta(minutes=11)
                )
            )
            == set()
        )


class TestBypassProjectsCache:
    now = datetime(2024, 5, 13, 9, 0, 0)

    @pytest.fixture(autouse=True)
    def reset_cache(self) -> Iterator[None]:
        replacements_and_expiry._cached_projects = {}
        replacements_and_expiry._cached_projects_at = None
        yield
        replacements_and_expiry._cached_projects = {}
        replacements_and_expiry._cached_projects_at = None

    @pytest.mark.redis_db
    def test_redis_is_read_once_within_the_ttl(self) -> None:
        set_config_auto_replacements_bypass_projects([1], self.now)

        with mock.patch.object(
            replacements_and_expiry,
            "_retrieve_projects_from_redis",
            wraps=replacements_and_expiry._retrieve_projects_from_redis,
        ) as retrieve:
            for _ in range(100):
                assert get_auto_replacements_bypass_projects_cached(self.now) == [1]
            assert retrieve.call_count == 1

    @pytest.mark.redis_db
    def test_expiry_is_exact_while_served_from_cache(self) -> None:
        expiry_window = get_option(REPLACEMENTS_EXPIRY_WINDOW_MINUTES_KEY, 5)
        assert expiry_window is not None
        set_config_auto_replacements_bypass_projects([1], self.now)

        with mock.patch.object(
            replacements_and_expiry,
            "_retrieve_projects_from_redis",
            wraps=replacements_and_expiry._retrieve_projects_from_redis,
        ) as retrieve:
            assert get_auto_replacements_bypass_projects_cached(self.now) == [1]
            # Same cached read, evaluated past the entry's expiry.
            assert (
                get_auto_replacements_bypass_projects_cached(
                    self.now + timedelta(minutes=expiry_window + 1)
                )
                == []
            )
            assert retrieve.call_count == 1

    @pytest.mark.redis_db
    def test_cache_refreshes_after_the_ttl(self) -> None:
        set_config_auto_replacements_bypass_projects([1], self.now)
        assert get_auto_replacements_bypass_projects_cached(self.now) == [1]

        # Invisible until the TTL lapses.
        set_config_auto_replacements_bypass_projects([2], self.now)
        assert get_auto_replacements_bypass_projects_cached(self.now) == [1]

        ttl = get_option(AUTO_REPLACEMENTS_BYPASS_CACHE_TTL_KEY, 5.0)
        assert ttl is not None
        with mock.patch("time.monotonic", return_value=time.monotonic() + ttl + 1):
            assert set(get_auto_replacements_bypass_projects_cached(self.now)) == {1, 2}
