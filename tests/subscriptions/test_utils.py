import time
from datetime import datetime, timedelta

from snuba import state
from snuba.datasets.entities import EntityKey
from snuba.subscriptions.utils import run_legacy_pipeline, run_new_pipeline


def test_rollout() -> None:
    now = int(time.time())
    past = datetime.fromtimestamp(now) - timedelta(minutes=1)
    future = datetime.fromtimestamp(now) + timedelta(minutes=1)

    entity = EntityKey.EVENTS

    # If no config set defaults to legacy
    assert run_new_pipeline(entity, past) is False
    assert run_new_pipeline(entity, future) is False
    assert run_legacy_pipeline(entity, past) is True
    assert run_legacy_pipeline(entity, future) is True

    # Set to legacy
    state.set_config("subscription_mode_events", "legacy")
    assert run_new_pipeline(entity, past) is False
    assert run_new_pipeline(entity, future) is False
    assert run_legacy_pipeline(entity, past) is True
    assert run_legacy_pipeline(entity, future) is True

    # Set to new
    state.set_config("subscription_mode_events", "new")
    assert run_new_pipeline(entity, past) is True
    assert run_new_pipeline(entity, future) is True
    assert run_legacy_pipeline(entity, past) is False
    assert run_legacy_pipeline(entity, future) is False

    # Transitioning to legacy, invalid transition time provided. Default to legacy.
    state.set_config("subscription_mode_events", "transition_legacy")
    assert run_new_pipeline(entity, past) is False
    assert run_new_pipeline(entity, future) is False
    assert run_legacy_pipeline(entity, past) is True
    assert run_legacy_pipeline(entity, future) is True

    # Transitioning to legacy with valid transition time
    state.set_config("subscription_transition_time_events", now)
    assert run_new_pipeline(entity, past) is False
    assert run_new_pipeline(entity, future) is True
    assert run_legacy_pipeline(entity, past) is True
    assert run_legacy_pipeline(entity, future) is False

    # Transitioining to new, invalid transition time provided. Default to legacy.
    state.set_config("subscription_mode_events", "transition_new")
    state.delete_config("subscription_transition_time_events")
    assert run_new_pipeline(entity, past) is False
    assert run_new_pipeline(entity, future) is False
    assert run_legacy_pipeline(entity, past) is True
    assert run_legacy_pipeline(entity, future) is True

    # Transitioning to new with valid transition time
    state.set_config("subscription_transition_time_events", now)
    assert run_new_pipeline(entity, past) is True
    assert run_new_pipeline(entity, future) is False
    assert run_legacy_pipeline(entity, past) is False
    assert run_legacy_pipeline(entity, future) is True

    # Invalid config only runs legacy
    state.set_config("subscription_mode_events", "ohno")
    state.set_config("subscription_transition_time_events", "ohno", force=True)
    assert run_new_pipeline(entity, past) is False
    assert run_new_pipeline(entity, future) is False
    assert run_legacy_pipeline(entity, past) is True
    assert run_legacy_pipeline(entity, future) is True

    # Run both pipelines, always true
    state.set_config("subscription_mode_events", "both")
    assert run_new_pipeline(entity, past) is True
    assert run_new_pipeline(entity, future) is True
    assert run_legacy_pipeline(entity, past) is True
    assert run_legacy_pipeline(entity, future) is True
