import pickle

from snuba.datasets.storages.factory import get_all_storage_keys, get_storage


def test_policies_are_pickleable() -> None:
    # * allocation policies will eventually become part of the Table object which queries are run against
    # * that table object is part of the query
    # * we need the query object to be deepcopy-able
    # * to deepcopy an object, all of its members must be pickleable

    # this test makes sure that any allocation policy defined on a storage is in fact pickleable
    for key in get_all_storage_keys():
        storage = get_storage(key)
        pickle.dumps(storage.get_allocation_policies())
