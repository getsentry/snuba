import sys
from dataclasses import dataclass
from typing import Any, Mapping, MutableSequence, Sequence, Type

from yaml import safe_load

from snuba.clickhouse.columns import ColumnSet
from snuba.datasets.entities.metrics import TagsTypeTransformer  # noqa
from snuba.datasets.entity import Entity
from snuba.query.processors import QueryProcessor, get_query_processor_by_name
from snuba.utils.schemas import (
    AggregateFunction,
    Column,
    ColumnType,
    DateTime,
    Float,
    Nested,
    SchemaModifiers,
    UInt,
)


@dataclass
class PluggableEntity(Entity):
    query_processors: Sequence[QueryProcessor]
    columns: ColumnSet

    def get_query_processors(self) -> Sequence[QueryProcessor]:
        return self.query_processors


SIMPLE_COLUMN_TYPE_MAPPING: Mapping[str, ColumnType[SchemaModifiers]] = {
    "Float(64)": Float(64),
    "UInt(64)": UInt(64),
    "DateTime": DateTime(),
}


def column_from_spec(yaml_spec: Any) -> Column[SchemaModifiers]:
    if yaml_spec["type"] == "Nested":
        subcolumns_spec = yaml_spec["subcolumns"]
        subcolumns = [
            (sub["name"], SIMPLE_COLUMN_TYPE_MAPPING[sub["type"]])
            for sub in subcolumns_spec
        ]
        return Column(yaml_spec["name"], Nested(subcolumns))
    elif yaml_spec["type"] == "AggregateFunction":
        return Column(
            yaml_spec["name"],
            AggregateFunction(
                yaml_spec["function"],
                [SIMPLE_COLUMN_TYPE_MAPPING[yaml_spec["datatype"]]],
            ),
        )
    return Column(yaml_spec["name"], SIMPLE_COLUMN_TYPE_MAPPING[yaml_spec["type"]])


def load_from_file(path: str) -> PluggableEntity:
    with open(path, "r") as f:
        unstructured_definition = safe_load(f)
        print(unstructured_definition)
        assert (
            unstructured_definition["kind"] == "entity"
        ), "non-entity cannot be loaded"
        assert unstructured_definition["spec"], "must contain spec"
        spec = unstructured_definition["spec"]
        query_processors: Sequence[Mapping[str, Any]] = spec["query_processors"]
        loaded_processors: MutableSequence[QueryProcessor] = []
        for qp in query_processors:
            for (name, args) in qp.items():
                processor_cls: Type[QueryProcessor] = get_query_processor_by_name(name)
                if args is None:
                    processor = processor_cls()
                else:
                    processor = processor_cls(**args)  # type: ignore
                assert isinstance(processor, QueryProcessor), "uh-oh"
                loaded_processors.append(processor)
        columns = [column_from_spec(c) for c in spec["columns"]]
        return PluggableEntity(
            query_processors=loaded_processors, columns=ColumnSet(columns)
        )


print(load_from_file(sys.argv[1]))
