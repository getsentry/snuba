from snuba import settings


def test_invalid_storage() -> None:
    from snuba.settings.validation import validate_settings

    # Build a dictionary with all variables defined in settings.
    all_settings = {
        key: value
        for key, value in settings.__dict__.items()
        if not key.startswith("__") and not callable(key)
    }
    cluster = all_settings["CLUSTERS"]
    cluster[0]["storage_sets"].add("non_existing_storage")
    try:
        validate_settings(all_settings)
    except Exception as exc:
        assert False, f"'validate_settings' raised an exception {exc}"
    finally:
        cluster[0]["storage_sets"].remove("non_existing_storage")


def test_topics_sync() -> None:
    from copy import deepcopy

    from snuba.settings.validation import validate_settings
    from snuba.utils.streams.topics import Topic

    all_settings = {
        key: value
        for key, value in settings.__dict__.items()
        if not key.startswith("__") and not callable(key)
    }
    topic_map = all_settings["KAFKA_TOPIC_MAP"]
    default_map = deepcopy(topic_map)
    # Remove default settings and build dummy Topic Map
    topic_map.clear()
    topic_map = {t.value: {} for t in Topic}

    try:
        validate_settings(all_settings)
    except Exception as exc:
        assert False, f"'validate_settings' raised an exception {exc}"
    finally:
        topic_map = default_map
