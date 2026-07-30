import copy
from collections.abc import Generator, Mapping, MutableMapping
from typing import Any

import jsonschema

Schema = Mapping[str, Any]  # placeholder for JSON schema


def _validate_and_default(
    validator: object,
    properties: Mapping[str, Any],
    instance: MutableMapping[str, Any],
    schema: Mapping[str, Any],
) -> Generator[Exception]:
    for property, subschema in properties.items():
        if property not in instance and "default" in subschema:
            if callable(subschema["default"]):
                default_value = subschema["default"]()
            else:
                default_value = copy.deepcopy(subschema["default"])
            instance[property] = default_value

    yield from jsonschema.Draft6Validator.VALIDATORS["properties"](
        validator, properties, instance, schema
    )


# validators.extend creates a new class, so do this once rather than for every query.
_VALIDATOR_WITH_DEFAULTS = jsonschema.validators.extend(
    jsonschema.Draft4Validator, {"properties": _validate_and_default}
)


def validate_jsonschema(
    value: MutableMapping[str, Any],
    schema: MutableMapping[str, Any],
    set_defaults: bool = True,
) -> MutableMapping[str, Any]:
    """
    Validates a value against the provided schema, returning the validated
    value if the value conforms to the schema, otherwise raising a
    ``jsonschema.ValidationError``.
    """
    # Using schema defaults during validation will cause the input value to be
    # mutated, so to be on the safe side we create a deep copy of that value to
    # avoid unwanted side effects for the calling function.
    if set_defaults:
        value = copy.deepcopy(value)

    validator_cls = _VALIDATOR_WITH_DEFAULTS if set_defaults else jsonschema.Draft6Validator

    validator_cls(
        schema,
        format_checker=jsonschema.FormatChecker(),
    ).validate(value, schema)

    return value
