from dataclasses import replace

from snuba.datasets.entities.entity_data_model import EntityColumnSet
from snuba.datasets.entities.entity_key import EntityKey
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.dsl import Functions as f
from snuba.query.dsl import NestedColumn, column, literal
from snuba.query.logical import Query
from snuba.query.processors.logical.low_cardinality_processor import (
    LowCardinalityProcessor,
)
from snuba.query.query_settings import HTTPQuerySettings

tags = NestedColumn("tags")


def test_low_cardinality_processor() -> None:
    unprocessed = Query(
        QueryEntity(EntityKey.EVENTS, EntityColumnSet([])),
        condition=f.equals(column("environment"), literal("production")),
    )
    expected = Query(
        QueryEntity(EntityKey.EVENTS, EntityColumnSet([])),
        condition=f.equals(
            f.cast(column("environment"), "Nullable(String)"), literal("production")
        ),
    )

    LowCardinalityProcessor(["environment"]).process_query(unprocessed, HTTPQuerySettings())
    assert expected.get_condition() == unprocessed.get_condition()


def test_low_cardinality_processor_with_tags() -> None:
    unprocessed = Query(
        QueryEntity(EntityKey.EVENTS, EntityColumnSet([])),
        condition=f.equals(tags["environment"], literal("production")),
    )
    expected = Query(
        QueryEntity(EntityKey.EVENTS, EntityColumnSet([])),
        condition=f.equals(
            f.cast(
                replace(tags["environment"], alias=None),
                "Nullable(String)",
                alias="_snuba_tags[environment]",
            ),
            literal("production"),
        ),
    )

    LowCardinalityProcessor(["tags[environment]"]).process_query(unprocessed, HTTPQuerySettings())
    assert expected.get_condition() == unprocessed.get_condition()
