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

SNQL_QUERY_SCHEMA = {
    "type": "object",
    "properties": {
        "query": {"type": "string"},
        "dataset": {"type": "string"},
    },
    "additionalProperties": False,
}

MQL_QUERY_SCHEMA = {
    "type": "object",
    "properties": {
        "query": {"type": "string"},
        "mql_context": {"type": "object"},
        "dataset": {"type": "string"},
    },
    "additionalProperties": False,
}
