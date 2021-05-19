from typing import Any, MutableMapping, cast

import pytest
from jsonschema.exceptions import ValidationError

from snuba.clickhouse.columns import ColumnSet
from snuba.datasets.entities import EntityKey
from snuba.query.conditions import ConditionFunctions, binary_condition
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.expressions import Column, Literal
from snuba.query.logical import Query
from snuba.query.organization_extension import OrganizationExtension
from snuba.request.request_settings import HTTPRequestSettings
from snuba.schemas import validate_jsonschema


def test_organization_extension_query_processing_happy_path() -> None:
    extension = OrganizationExtension()
    schema = cast(MutableMapping[str, Any], extension.get_schema())
    raw_data = {"organization": 2}

    valid_data = validate_jsonschema(raw_data, schema)
    query = Query(QueryEntity(EntityKey.EVENTS, ColumnSet([])))
    request_settings = HTTPRequestSettings()

    extension.get_processor().process_query(query, valid_data, request_settings)
    assert query.get_condition() == binary_condition(
        ConditionFunctions.EQ, Column("_snuba_org_id", None, "org_id"), Literal(None, 2)
    )


def test_invalid_data_does_not_validate() -> None:
    extension = OrganizationExtension()
    schema = cast(MutableMapping[str, Any], extension.get_schema())

    with pytest.raises(ValidationError):
        validate_jsonschema({"organization": "2"}, schema)

    with pytest.raises(ValidationError):
        validate_jsonschema({"organization": 0}, schema)

    with pytest.raises(ValidationError):
        validate_jsonschema({"organization": [2]}, schema)
