from typing import Mapping

from snuba import settings
from snuba.datasets.cdc import CdcStorage
from snuba.datasets.configuration.storage_builder import build_storage
from snuba.datasets.storage import ReadableTableStorage, WritableTableStorage
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.discover import storage as discover_storage
from snuba.datasets.storages.errors import storage as errors_storage
from snuba.datasets.storages.errors_ro import storage as errors_ro_storage
from snuba.datasets.storages.errors_v2 import storage as errors_v2_storage
from snuba.datasets.storages.errors_v2_ro import storage as errors_v2_ro_storage
from snuba.datasets.storages.functions import agg_storage as functions_ro_storage
from snuba.datasets.storages.functions import raw_storage as functions_storage
from snuba.datasets.storages.generic_metrics import (
    distributions_bucket_storage as gen_metrics_dists_bucket_storage_original,
)
from snuba.datasets.storages.generic_metrics import (
    distributions_storage as gen_metrics_dists_aggregate_storage_original,
)
from snuba.datasets.storages.generic_metrics import (
    sets_bucket_storage as gen_metrics_sets_bucket_storage_original,
)
from snuba.datasets.storages.generic_metrics import (
    sets_storage as gen_metrics_sets_aggregate_storage_original,
)
from snuba.datasets.storages.groupassignees import storage as groupassignees_storage
from snuba.datasets.storages.groupedmessages import storage as groupedmessages_storage
from snuba.datasets.storages.metrics import counters_storage as metrics_counters_storage
from snuba.datasets.storages.metrics import (
    distributions_storage as metrics_distributions_storage,
)
from snuba.datasets.storages.metrics import (
    org_counters_storage as metrics_org_counters_storage,
)
from snuba.datasets.storages.metrics import (
    polymorphic_bucket as metrics_polymorphic_storage,
)
from snuba.datasets.storages.metrics import sets_storage as metrics_sets_storage
from snuba.datasets.storages.outcomes import (
    materialized_storage as outcomes_hourly_storage,
)
from snuba.datasets.storages.outcomes import raw_storage as outcomes_raw_storage
from snuba.datasets.storages.profiles import (
    writable_storage as profiles_writable_storage,
)
from snuba.datasets.storages.querylog import storage as querylog_storage
from snuba.datasets.storages.replays import storage as replays_storage
from snuba.datasets.storages.sessions import (
    materialized_storage as sessions_hourly_storage,
)
from snuba.datasets.storages.sessions import (
    org_materialized_storage as org_sessions_hourly_storage,
)
from snuba.datasets.storages.sessions import raw_storage as sessions_raw_storage
from snuba.datasets.storages.transactions import storage as transactions_storage
from snuba.datasets.storages.transactions_ro import storage as transactions_ro_storage
from snuba.datasets.storages.transactions_v2 import storage as transactions_v2_storage
from snuba.state import get_config

gen_metrics_dists_bucket_storage = gen_metrics_dists_bucket_storage_original
gen_metrics_dists_aggregate_storage = gen_metrics_dists_aggregate_storage_original
gen_metrics_sets_bucket_storage = gen_metrics_sets_bucket_storage_original
gen_metrics_sets_aggregate_storage = gen_metrics_sets_aggregate_storage_original

if get_config("use_generic_metrics_storages_from_configs", 0):
    dists_storage_new = build_storage(StorageKey.GENERIC_METRICS_DISTRIBUTIONS_RAW)
    assert isinstance(dists_storage_new, WritableTableStorage)
    gen_metrics_dists_bucket_storage = dists_storage_new
    gen_metrics_dists_aggregate_storage = build_storage(
        StorageKey.GENERIC_METRICS_DISTRIBUTIONS
    )
    sets_storage_new = build_storage(StorageKey.GENERIC_METRICS_SETS_RAW)
    assert isinstance(sets_storage_new, WritableTableStorage)
    gen_metrics_sets_bucket_storage = sets_storage_new
    gen_metrics_sets_aggregate_storage = build_storage(StorageKey.GENERIC_METRICS_SETS)


DEV_CDC_STORAGES: Mapping[StorageKey, CdcStorage] = {}

CDC_STORAGES: Mapping[StorageKey, CdcStorage] = {
    **{
        storage.get_storage_key(): storage
        for storage in [groupedmessages_storage, groupassignees_storage]
    },
    **(DEV_CDC_STORAGES if settings.ENABLE_DEV_FEATURES else {}),
}

DEV_WRITABLE_STORAGES: Mapping[StorageKey, WritableTableStorage] = {}

METRICS_WRITEABLE_STORAGES = {
    metrics_distributions_storage.get_storage_key(): metrics_distributions_storage,
    metrics_sets_storage.get_storage_key(): metrics_sets_storage,
    metrics_counters_storage.get_storage_key(): metrics_counters_storage,
    metrics_polymorphic_storage.get_storage_key(): metrics_polymorphic_storage,
}

WRITABLE_STORAGES: Mapping[StorageKey, WritableTableStorage] = {
    **CDC_STORAGES,
    **METRICS_WRITEABLE_STORAGES,
    **{
        storage.get_storage_key(): storage
        for storage in [
            errors_storage,
            outcomes_raw_storage,
            querylog_storage,
            sessions_raw_storage,
            transactions_storage,
            transactions_v2_storage,
            errors_v2_storage,
            profiles_writable_storage,
            functions_storage,
            gen_metrics_sets_bucket_storage,
            replays_storage,
            gen_metrics_dists_bucket_storage,
        ]
    },
    **(DEV_WRITABLE_STORAGES if settings.ENABLE_DEV_FEATURES else {}),
}

DEV_NON_WRITABLE_STORAGES: Mapping[StorageKey, ReadableTableStorage] = {}

METRICS_NON_WRITABLE_STORAGES: Mapping[StorageKey, ReadableTableStorage] = {
    metrics_counters_storage.get_storage_key(): metrics_counters_storage,
    metrics_distributions_storage.get_storage_key(): metrics_distributions_storage,
    metrics_org_counters_storage.get_storage_key(): metrics_org_counters_storage,
    metrics_sets_storage.get_storage_key(): metrics_sets_storage,
    gen_metrics_sets_aggregate_storage.get_storage_key(): gen_metrics_sets_aggregate_storage,
    gen_metrics_dists_aggregate_storage.get_storage_key(): gen_metrics_dists_aggregate_storage,
}

NON_WRITABLE_STORAGES: Mapping[StorageKey, ReadableTableStorage] = {
    **METRICS_NON_WRITABLE_STORAGES,
    **{
        storage.get_storage_key(): storage
        for storage in [
            discover_storage,
            errors_ro_storage,
            outcomes_hourly_storage,
            sessions_hourly_storage,
            org_sessions_hourly_storage,
            transactions_ro_storage,
            profiles_writable_storage,
            functions_ro_storage,
            errors_v2_ro_storage,
        ]
    },
    **(DEV_NON_WRITABLE_STORAGES if settings.ENABLE_DEV_FEATURES else {}),
}

STORAGES: Mapping[StorageKey, ReadableTableStorage] = {
    **WRITABLE_STORAGES,
    **NON_WRITABLE_STORAGES,
}


def get_storage(storage_key: StorageKey) -> ReadableTableStorage:
    return STORAGES[storage_key]


def get_writable_storage(storage_key: StorageKey) -> WritableTableStorage:
    return WRITABLE_STORAGES[storage_key]


def get_cdc_storage(storage_key: StorageKey) -> CdcStorage:
    return CDC_STORAGES[storage_key]
