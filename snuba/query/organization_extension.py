from snuba.query.conditions import binary_condition, ConditionFunctions
from snuba.query.expressions import Column, Literal
from snuba.query.extensions import QueryExtension
from snuba.query.logical import Query
from snuba.query.processors import ExtensionData, ExtensionQueryProcessor
from snuba.request.request_settings import RequestSettings


ORGANIZATION_EXTENSION_SCHEMA = {
    "type": "object",
    "properties": {"organization": {"type": "integer", "minimum": 1}},
    "required": ["organization"],
    "additionalProperties": False,
}


class OrganizationExtensionProcessor(ExtensionQueryProcessor):
    """
    Extension processor for datasets that require an organization ID to be given in the request.
    """

    def process_query(
        self,
        query: Query,
        extension_data: ExtensionData,
        request_settings: RequestSettings,
    ) -> None:
        organization_id = extension_data["organization"]
        query.add_condition_to_ast(
            binary_condition(
                ConditionFunctions.EQ,
                Column("_snuba_org_id", None, "org_id"),
                Literal(None, organization_id),
            )
        )


class OrganizationExtension(QueryExtension):
    def __init__(self) -> None:
        super().__init__(
            schema=ORGANIZATION_EXTENSION_SCHEMA,
            processor=OrganizationExtensionProcessor(),
        )
