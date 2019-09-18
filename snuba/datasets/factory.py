from snuba import settings

DATASETS_IMPL = {}

DATASET_NAMES = {
    'events',
    'groupedmessage',
    'transactions',
    'outcomes',
}


class InvalidDatasetError(Exception):
    """Exception raised on invalid dataset access."""


def get_dataset(name):
    if name in DATASETS_IMPL:
        return DATASETS_IMPL[name]

    if name in settings.DISABLED_DATASETS:
        raise InvalidDatasetError(f"dataset {name!r} is not available in this environment")

    from snuba.datasets.events import EventsDataset
    from snuba.datasets.cdc.groupedmessage import GroupedMessageDataset
    from snuba.datasets.transactions import TransactionsDataset
    from snuba.datasets.outcomes import OutcomesDataset
    from snuba.datasets.eventsv2 import EventsV2
    dataset_mappings = {
        'events': EventsDataset,
        'groupedmessage': GroupedMessageDataset,
        'transactions': TransactionsDataset,
        'outcomes': OutcomesDataset,
        'eventsv2': EventsV2,
    }

    try:
        dataset = DATASETS_IMPL[name] = dataset_mappings[name]()
    except KeyError as error:
        raise InvalidDatasetError(f"dataset {name!r} does not exist") from error

    return dataset


def get_enabled_dataset_names():
    return [name for name in DATASET_NAMES if name not in settings.DISABLED_DATASETS]
