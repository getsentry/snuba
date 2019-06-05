from snuba import settings
from snuba.datasets.events import EventsDataSet

DATASETS_IMPL = {}

DATASETS_MAPPING = {
    'events': EventsDataSet,
}


def get_dataset(name):
    if name in DATASETS_IMPL:
        return DATASETS_IMPL[name]

    assert name not in settings.DISABLED_DATASETS, "Dataset %s not available in this environment" % name

    dataset = DATASETS_IMPL[name] = DATASETS_MAPPING[name]()
    return dataset


def get_enabled_dataset_names():
    return [name for name in DATASETS_MAPPING.keys() if name not in settings.DISABLED_DATASETS]
