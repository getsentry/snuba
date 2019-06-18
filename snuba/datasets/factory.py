from snuba import settings

DATASETS_IMPL = {}

DATASET_NAMES = {
    'events',
    'groupedmessage',
}


def get_dataset(name):
    if name in DATASETS_IMPL:
        return DATASETS_IMPL[name]

    assert name not in settings.DISABLED_DATASETS, "Dataset %s not available in this environment" % name

    from snuba.datasets.events import EventsDataSet
    from snuba.datasets.cdc.groupedmessage import GroupedMessageDataSet
    dataset_mappings = {
        'events': EventsDataSet,
        'groupedmessage': GroupedMessageDataSet
    }

    dataset = DATASETS_IMPL[name] = dataset_mappings[name]()
    return dataset


def get_enabled_dataset_names():
    return [name for name in DATASET_NAMES if name not in settings.DISABLED_DATASETS]
