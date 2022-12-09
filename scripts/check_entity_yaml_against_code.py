import sys

from snuba.datasets.configuration.entity_builder import build_entity_from_config
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity


def verify_schemas(converted_entity, original_entity):
    assert converted_entity.init_kwargs == original_entity.init_kwargs


def check_yaml_against_code(entity_key_str, yaml_file_path):
    converted_entity = build_entity_from_config(yaml_file_path)
    original_entity = get_entity(EntityKey(entity_key_str))
    verify_schemas(converted_entity, original_entity)


if __name__ == "__main__":
    check_yaml_against_code(sys.argv[1], sys.argv[2])
    print("They match!")
