from typing import Final, Mapping, Set

from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey

from snuba.query.dsl import Functions as f
from snuba.query.dsl import column, literal
from snuba.query.expressions import Expression
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException

# These are the columns which aren't stored in attr_ in clickhouse
NORMALIZED_COLUMNS: Final[Mapping[str, AttributeKey.Type.ValueType]] = {
    "sentry.organization_id": AttributeKey.Type.TYPE_INT,
    "sentry.project_id": AttributeKey.Type.TYPE_INT,
}

TIMESTAMP_COLUMNS: Final[Set[str]] = {
    "sentry.timestamp",
}


def attribute_key_to_expression(attr_key: AttributeKey) -> Expression:
    if attr_key.type == AttributeKey.Type.TYPE_UNSPECIFIED:
        raise BadSnubaRPCRequestException(
            f"attribute key {attr_key.name} must have a type specified"
        )
    alias = attr_key.name + "_" + AttributeKey.Type.Name(attr_key.type)

    if attr_key.name == "sentry.trace_id":
        if attr_key.type == AttributeKey.Type.TYPE_STRING:
            return f.CAST(column("trace_id"), "String", alias=alias)
        raise BadSnubaRPCRequestException(
            f"Attribute {attr_key.name} must be requested as a string, got {attr_key.type}"
        )

    if attr_key.name == "sentry.item_id":
        if attr_key.type == AttributeKey.Type.TYPE_STRING:
            return column("item_id", alias=alias)
        raise BadSnubaRPCRequestException(
            f"Attribute {attr_key.name} must be requested as a string, got {attr_key.type}"
        )

    if attr_key.name in TIMESTAMP_COLUMNS:
        if attr_key.type == AttributeKey.Type.TYPE_STRING:
            return f.CAST(
                column(attr_key.name[len("sentry.") :]), "String", alias=alias
            )
        if attr_key.type == AttributeKey.Type.TYPE_INT:
            return f.CAST(column(attr_key.name[len("sentry.") :]), "Int64", alias=alias)
        if attr_key.type == AttributeKey.Type.TYPE_FLOAT:
            return f.CAST(
                column(attr_key.name[len("sentry.") :]), "Float64", alias=alias
            )
        raise BadSnubaRPCRequestException(
            f"Attribute {attr_key.name} must be requested as a string, float, or integer, got {attr_key.type}"
        )

    if attr_key.name in NORMALIZED_COLUMNS:
        if NORMALIZED_COLUMNS[attr_key.name] == attr_key.type:
            return column(attr_key.name[len("sentry.") :], alias=attr_key.name)
        raise BadSnubaRPCRequestException(
            f"Attribute {attr_key.name} must be requested as {NORMALIZED_COLUMNS[attr_key.name]}, got {attr_key.type}"
        )

    # End of special handling, just send to the appropriate bucket
    if attr_key.type == AttributeKey.Type.TYPE_STRING:
        return f.arrayElement(
            column("attributes_string"), literal(attr_key.name), alias=alias
        )
    if attr_key.type == AttributeKey.Type.TYPE_FLOAT:
        return f.arrayElement(
            column("attributes_float"), literal(attr_key.name), alias=alias
        )
    if attr_key.type == AttributeKey.Type.TYPE_INT:
        return f.arrayElement(
            column("attributes_int"), literal(attr_key.name), alias=alias
        )
    if attr_key.type == AttributeKey.Type.TYPE_BOOLEAN:
        return f.arrayElement(
            column("attributes_bool"), literal(attr_key.name), alias=alias
        )
    raise BadSnubaRPCRequestException(
        f"Attribute {attr_key.name} had an unknown or unset type: {attr_key.type}"
    )
