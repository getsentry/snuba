from snuba.datasets.factory import get_dataset, get_enabled_dataset_names
from snuba.datasets.storage import WritableStorage
from snuba.web.views import check_clickhouse


def test_something() -> None:
    assert check_clickhouse(filter_experimental=True)


# def test_dataset_storages() -> None:
#     from pprint import pprint as print

#     dataset_storages = {}
#     for dataset_name in get_enabled_dataset_names():
#         dataset = get_dataset(dataset_name)
#         dataset_storages[dataset_name] = []
#         for entity in dataset.get_all_entities():
#             for storage in entity.get_all_storages():
#                 dataset_storages[dataset_name].append(storage.get_storage_key())

#     print(dataset_storages)
