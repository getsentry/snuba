import copy
from typing import Any, Mapping, MutableMapping

import jsonschema


Schema = Mapping[str, Any]  # placeholder for JSON schema


def validate_jsonschema(value, schema, set_defaults=True):
    """
    Validates a value against the provided schema, returning the validated
    value if the value conforms to the schema, otherwise raising a
    ``jsonschema.ValidationError``.
    """
    orig = jsonschema.Draft6Validator.VALIDATORS["properties"]

    def validate_and_default(
        validator,
        properties: Mapping[str, Any],
        instance: MutableMapping[str, Any],
        schema,
    ):
        for property, subschema in properties.items():
            if property not in instance and "default" in subschema:
                if callable(subschema["default"]):
                    default_value = subschema["default"]()
                else:
                    default_value = copy.deepcopy(subschema["default"])
                instance[property] = default_value

        for error in orig(validator, properties, instance, schema):
            yield error

    # Using schema defaults during validation will cause the input value to be
    # mutated, so to be on the safe side we create a deep copy of that value to
    # avoid unwanted side effects for the calling function.
    if set_defaults:
        value = copy.deepcopy(value)

    validator_cls = (
        jsonschema.validators.extend(
            jsonschema.Draft4Validator, {"properties": validate_and_default}
        )
        if set_defaults
        else jsonschema.Draft6Validator
    )

    validator_cls(
        schema,
        types={"array": (list, tuple)},
        format_checker=jsonschema.FormatChecker(),
    ).validate(value, schema)

    return value
