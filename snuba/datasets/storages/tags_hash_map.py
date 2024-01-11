# This obnoxious amount backslashes is sadly required to escape the tag keys.
# replaceRegexpAll(k, '(\\\\=|\\\\\\\\)', '\\\\\\\\\\\\1')
# means running this on Clickhouse:
# replaceRegexpAll(k, '(\\=|\\\\)', '\\\\\\1')
# The (\\=|\\\\) pattern should be actually this: (|\=|\\). The additional
# backslashes are because of Clickhouse escaping.
TAGS_HASH_MAP_COLUMN = (
    "arrayMap((k, v) -> cityHash64(concat("
    "replaceRegexpAll(k, '(\\\\=|\\\\\\\\)', '\\\\\\\\\\\\1'), '=', v)), "
    "tags.key, tags.value)"
)

CONTEXTS_HASH_MAP_COLUMN = (
    "arrayMap((k, v) -> cityHash64(concat("
    "replaceRegexpAll(k, '(\\\\=|\\\\\\\\)', '\\\\\\\\\\\\1'), '=', v)), "
    "contexts.key, contexts.value)"
)

# There an issue in Clickhouse where the arrayMap function passes
# in Nothing type values for empty arrays. This causes the regex function to fail
# without the toString function, unless a merge for the part is completed.
# This is fixed in Clickhouse 22.
SENTRY_TAGS_HASH_MAP_COLUMN = (
    "arrayMap((k, v) -> cityHash64(concat("
    "replaceRegexpAll(toString(k), '(\\\\=|\\\\\\\\)', '\\\\\\\\\\\\1'), '=', toString(v))), "
    "sentry_tags.key, sentry_tags.value)"
)


def hash_map_int_column_definition(key_column_name: str, value_column_name: str) -> str:
    return (
        f"arrayMap((k, v) -> cityHash64(concat(toString(k), '=', toString(v))), "
        f"{key_column_name}, {value_column_name})"
    )


INT_TAGS_HASH_MAP_COLUMN = hash_map_int_column_definition("tags.key", "tags.value")


def hash_map_int_key_str_value_column_definition(
    key_column_name: str, value_column_name: str
) -> str:
    return (
        f"arrayMap((k, v) -> cityHash64(concat(toString(k), '=', v)), "
        f"{key_column_name}, {value_column_name})"
    )
