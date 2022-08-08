import sys

from yaml import safe_load


class EntityDefinition:
    pass


def load_from_file(path: str) -> EntityDefinition:
    with open(path, "r") as f:
        unstructured_definition = safe_load(f)
        assert (
            unstructured_definition["kind"] == "entity"
        ), "non-entity cannot be loaded"
        assert unstructured_definition["spec"], "must contain spec"
        return EntityDefinition()


print(load_from_file(sys.argv[1]))
