import pytest

from snuba.web.rpc.v1.resolvers.R_eap_items.storage_routing.load_retriever import get_cluster_load

@pytest.mark.redis_db
@pytest.mark.clickhouse_db
def test_get_cluster_load() -> None:
    load_info = get_cluster_load()
    assert load_info is not None
    assert load_info.cluster_load is not None
    assert load_info.concurrent_queries is not None
