import sys
from dataclasses import dataclass
from typing import Any, Mapping, MutableSequence, Sequence, Type

from yaml import safe_load

from snuba.datasets.entities.metrics import TagsTypeTransformer  # noqa
from snuba.query.processors import QueryProcessor, get_query_processor_by_name


@dataclass
class EntityDefinition:
    query_processors: Sequence[QueryProcessor]


def load_from_file(path: str) -> EntityDefinition:
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
                loaded_processors.append(processor)
        return EntityDefinition(query_processors=loaded_processors)


print(load_from_file(sys.argv[1]))
