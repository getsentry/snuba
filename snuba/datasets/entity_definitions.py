import sys
from typing import Any, Mapping, Sequence, Type

from yaml import safe_load

from snuba.datasets.entities.metrics import TagsTypeTransformer  # noqa
from snuba.query.processors import QueryProcessor, get_query_processor_by_name


class EntityDefinition:
    pass


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
        for qp in query_processors:
            for (name, args) in qp.items():
                processor_cls: Type[QueryProcessor] = get_query_processor_by_name(name)
                if args is None:
                    processor = processor_cls()
                else:
                    print(args)
                    processor = processor_cls(**args)
                print(processor)
        return EntityDefinition()


print(load_from_file(sys.argv[1]))
