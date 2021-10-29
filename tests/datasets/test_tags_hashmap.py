from functools import partial

import pytest

from snuba.clickhouse.query import Query
from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.entity import Entity
from snuba.datasets.factory import get_dataset
from snuba.query.expressions import Column, FunctionCall, Literal, StringifyVisitor
from snuba.reader import Reader
from snuba.request import Language
from snuba.request.request_settings import HTTPRequestSettings, RequestSettings
from snuba.request.schema import RequestSchema
from snuba.request.validation import build_request, parse_snql_query
from snuba.utils.metrics.timer import Timer

#   OTHER QUERY:[
#       "ChainMap({'query': \"MATCH (discover SAMPLE 1.0) SELECT count() AS count BY tags_key WHERE timestamp",
#       ">= toDateTime('2021-07-15T19:42:37.365384') AND timestamp < toDateTime('2021-10-13T19:41:50') AND",
#       "project_id IN tuple(1) AND project_id IN tuple(1) ORDER BY count DESC LIMIT 1000\", 'dataset':",
#       "'discover', 'parent_api': '/api/0/organizations/{organization_slug}/tags/'})"
#   ]


# [
#     "SELECT (toDateTime(multiply(intDiv(toUInt32(timestamp), 14400), 14400), 'Universal') AS _snuba_time), (count() AS _snuba_count)",
#     [
#         "FROM",
#         "discover_dist"
#     ],
#     "PREWHERE in((project_id AS _snuba_project_id), tuple(1))",
#     "WHERE equals(deleted, 0) AND greaterOrEquals((timestamp AS _snuba_timestamp), toDateTime('2021-07-10T17:41:55', 'Universal')) AND less(_snuba_timestamp, toDateTime('2021-10-08T17:30:01', 'Universal')) AND has(tags.key, 'organization.slug') AND in(ifNull((arrayElement(tags.value, indexOf(tags.key, 'subscription.plan')) AS `_snuba_tags[subscription.plan]`), ''), tuple('am1_business', 'am1_business_auf', 'am1_business_ent', 'am1_business_ent_auf'))",
#     "GROUP BY _snuba_time",
#     "ORDER BY _snuba_time ASC",
#     "LIMIT 10000 OFFSET 0"
# ]


# [
#     "ChainMap({'query': \"MATCH (discover) SELECT uniq(user) AS count_unique_user WHERE timestamp >=",
#     "toDateTime('2021-09-08T17:30:01') AND timestamp < toDateTime('2021-10-08T17:30:01') AND project_id",
#     "IN tuple(1) AND ifNull(tags[organization.slug], '') != '' AND ifNull(tags[subscription.plan], '') IN",
#     "tuple('am1_business', 'am1_business_auf', 'am1_business_ent', 'am1_business_ent_auf') AND type =",
#     "'transaction' AND match(transaction, '(?i)^/api/0/.*$') = 1 AND project_id IN tuple(1) LIMIT 51",
#     "OFFSET 0\", 'dataset': 'discover', 'parent_api':",
#     "'/api/0/organizations/{organization_slug}/eventsv2/'})"
# ]


# LOOK AT ME!!!!: WE don't support IN, and that makes the queries not great


def test_tags_hashmap() -> None:
    entity = get_entity(EntityKey.DISCOVER)
    dataset_name = "discover"
    query_str = """
    MATCH (discover)
    SELECT count() AS count
    WHERE
        timestamp >= toDateTime('2021-07-12T19:45:01') AND
        timestamp < toDateTime('2021-08-11T19:45:01') AND
        project_id IN tuple(300688) AND
        type = 'transaction' AND ifNull(tags[duration_group], '') != '' AND
        ifNull(tags[duration_group], '') = '<10s' AND
        transaction_status = 0 AND
        duration > 500.0 AND
        project_id IN tuple(300688)
    LIMIT 50
    """
    # ----- create the request object as if it came in through our API -----
    query_body = {
        "query": query_str,
        "debug": True,
        "dataset": dataset_name,
        "turbo": False,
        "consistent": False,
    }

    dataset = get_dataset(dataset_name)
    parser = partial(parse_snql_query, [])

    schema = RequestSchema.build_with_extensions(
        entity.get_extensions(), HTTPRequestSettings, Language.SNQL,
    )

    request = build_request(
        query_body,
        parser,
        HTTPRequestSettings,
        schema,
        dataset,
        Timer(name="bloop"),
        "some_referrer",
    )
    # --------------------------------------------------------------------

    def query_verifier(query: Query, settings: RequestSettings, reader: Reader) -> None:
        # The only reason this extends StringifyVisitor is because it has all the other
        # visit methods implemented.
        print(query)

    entity.get_query_pipeline_builder().build_execution_pipeline(
        request, query_verifier
    ).execute()
