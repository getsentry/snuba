from functools import lru_cache

from snuba.datasets.storages.storage_key import StorageKey

IDENTICAL_STORAGES = frozenset(
    {
        frozenset({StorageKey.TRANSACTIONS, StorageKey.TRANSACTIONS_V2}),
        frozenset({StorageKey.ERRORS, StorageKey.ERRORS_V2}),
    }
)


@lru_cache(20)
def are_writes_identical(this: StorageKey, other: StorageKey) -> bool:
    for group in IDENTICAL_STORAGES:
        if {this, other}.issubset(group):
            return True

    return False
