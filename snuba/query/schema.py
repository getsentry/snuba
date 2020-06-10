CONDITION_OPERATORS = [
    ">",
    "<",
    ">=",
    "<=",
    "=",
    "!=",
    "IN",
    "NOT IN",
    "IS NULL",
    "IS NOT NULL",
    "LIKE",
    "NOT LIKE",
]
POSITIVE_OPERATORS = [">", "<", ">=", "<=", "=", "IN", "IS NULL", "LIKE"]
UNARY_OPERATORS = ["IS NULL", "IS NOT NULL"]
BINARY_OPERATORS = [o for o in CONDITION_OPERATORS if o not in UNARY_OPERATORS]

GENERIC_QUERY_SCHEMA = {
    "type": "object",
    "properties": {
        "selected_columns": {
            "anyOf": [
                {"$ref": "#/definitions/column_name"},
                {"$ref": "#/definitions/column_list"},
                {"type": "array", "minItems": 0, "maxItems": 0},
            ],
            "default": [],
        },
        "aggregations": {
            "type": "array",
            "items": {
                "type": "array",
                "items": [
                    {
                        # Aggregation function
                        # TODO this should eventually become more restrictive again.
                        "type": "string",
                    },
                    {
                        # Aggregate column
                        "anyOf": [
                            {"$ref": "#/definitions/column_list"},
                            {"$ref": "#/definitions/column_name"},
                            {"enum": [""]},
                            {"type": "null"},
                        ],
                    },
                    {
                        # Alias
                        "type": ["string", "null"],
                    },
                ],
                "minItems": 3,
                "maxItems": 3,
            },
            "default": [],
        },
        "arrayjoin": {"$ref": "#/definitions/column_name"},
        "sample": {
            "anyOf": [
                {"type": "integer", "minimum": 0},
                {"type": "number", "minimum": 0.0, "maximum": 1.0},
            ],
        },
        "conditions": {
            "type": "array",
            "items": {
                "anyOf": [
                    {"$ref": "#/definitions/condition"},
                    {"type": "array", "items": {"$ref": "#/definitions/condition"}},
                ],
            },
            "default": [],
        },
        "groupby": {
            "anyOf": [
                {"$ref": "#/definitions/column_name"},
                {"$ref": "#/definitions/column_list"},
                {"type": "array", "maxItems": 0},
            ],
            "default": [],
        },
        "totals": {"type": "boolean", "default": False},
        "having": {
            "type": "array",
            # HAVING looks just like a condition
            "items": {"$ref": "#/definitions/condition"},
            "default": [],
        },
        "orderby": {
            "anyOf": [
                {"$ref": "#/definitions/column_name"},
                {"$ref": "#/definitions/nested_expr"},
                {
                    "type": "array",
                    "items": {
                        "anyOf": [
                            {"$ref": "#/definitions/column_name"},
                            {"$ref": "#/definitions/nested_expr"},
                        ],
                    },
                },
            ]
        },
        "limit": {"type": "integer", "default": 1000, "minimum": 0, "maximum": 10000},
        "offset": {"type": "integer", "minimum": 0},
        "limitby": {
            "type": "array",
            "items": [
                {"type": "integer", "minimum": 0},
                {"$ref": "#/definitions/column_name"},
            ],
        },
    },
    "dependencies": {"offset": ["limit"], "totals": ["groupby"]},
    "additionalProperties": False,
    "definitions": {
        "column_name": {
            "type": "string",
            "anyOf": [
                # This supports ClickHouse identifiers (with the addition of
                # the dot operator for referencing Nested columns, either in
                # aggregate or after an ARRAY JOIN), as well as an optional
                # subscript component at the end of the identifier (surrounded
                # by square brackets, e.g. `tags[key]`) that can be used for
                # treating Nested columns as mapping types in some contexts.
                {"pattern": r"^-?[a-zA-Z0-9_.]+(\[[a-zA-Z0-9_.:-]+\])?$"},
            ],
        },
        "column_list": {
            "type": "array",
            "items": {
                "anyOf": [
                    {"$ref": "#/definitions/column_name"},
                    {"$ref": "#/definitions/nested_expr"},
                ]
            },
            "minItems": 1,
        },
        # TODO: can the complex nested expr actually be encoded here?
        "nested_expr": {"type": "array"},
        # A condition is a 3-tuple of (column, operator, literal)
        # `conditions` is an array of conditions, or an array of arrays of conditions.
        # Conditions at the the top level are ANDed together.
        # Conditions at the second level are ORed together.
        # eg: [(a, =, 1), (b, =, 2)] => "a = 1 AND b = 2"
        # eg: [(a, =, 1), [(b, =, 2), (c, =, 3)]] => "a = 1 AND (b = 2 OR c = 3)"
        "condition_lhs": {
            "anyOf": [
                {"$ref": "#/definitions/column_name"},
                {"$ref": "#/definitions/nested_expr"},
            ],
        },
        "binary_condition": {
            "type": "array",
            "items": [
                {"$ref": "#/definitions/condition_lhs"},
                {
                    # Operator
                    "type": "string",
                    "enum": BINARY_OPERATORS,
                },
                {
                    # Literal
                    "anyOf": [
                        {"type": ["string", "number"]},
                        {"type": "array", "items": {"type": ["string", "number"]}},
                    ],
                },
            ],
            "minItems": 3,
            "maxItems": 3,
        },
        "unary_condition": {
            "type": "array",
            "items": [
                {"$ref": "#/definitions/condition_lhs"},
                {
                    # Operator
                    "type": "string",
                    "enum": UNARY_OPERATORS,
                },
                {
                    # Literal
                    "type": "null",
                },
            ],
            "minItems": 3,
            "maxItems": 3,
        },
        "condition": {
            "anyOf": [
                {"$ref": "#/definitions/binary_condition"},
                {"$ref": "#/definitions/unary_condition"},
            ],
        },
    },
}
