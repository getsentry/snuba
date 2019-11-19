import copy
from datetime import datetime, timedelta
from typing import Any, Mapping

import jsonschema


def get_time_series_extension_properties(
    default_granularity: int, default_window: timedelta
):
    return {
        "type": "object",
        "properties": {
            "from_date": {
                "type": "string",
                "format": "date-time",
                "default": lambda: (
                    datetime.utcnow().replace(microsecond=0) - default_window
                ).isoformat(),
            },
            "to_date": {
                "type": "string",
                "format": "date-time",
                "default": lambda: datetime.utcnow().replace(microsecond=0).isoformat(),
            },
            "granularity": {
                "type": "number",
                "default": default_granularity,
                "minimum": 1,
            },
        },
        "additionalProperties": False,
    }


Schema = Mapping[str, Any]  # placeholder for JSON schema


SDK_STATS_BASE_SCHEMA = {
    "type": "object",
    "properties": {
        "groupby": {
            "type": "array",
            "items": {
                # at the moment the only additional thing you can group by is project_id
                "enum": ["project_id"]
            },
            "default": [],
        },
    },
    "additionalProperties": False,
}

SDK_STATS_EXTENSIONS_SCHEMA = {
    "timeseries": get_time_series_extension_properties(
        default_granularity=86400,  # SDK stats query defaults to 1-day bucketing
        default_window=timedelta(days=1),
    ),
}


def validate_jsonschema(value, schema, set_defaults=True):
    """
    Validates a value against the provided schema, returning the validated
    value if the value conforms to the schema, otherwise raising a
    ``jsonschema.ValidationError``.
    """
    orig = jsonschema.Draft6Validator.VALIDATORS["properties"]

    def validate_and_default(validator, properties, instance, schema):
        for property, subschema in properties.items():
            if "default" in subschema:
                if callable(subschema["default"]):
                    instance.setdefault(property, subschema["default"]())
                else:
                    instance.setdefault(property, copy.deepcopy(subschema["default"]))

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
