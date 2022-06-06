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


INT_TAGS_HASH_MAP_COLUMN = (
    "arrayMap((k, v) -> cityHash64(concat(toString(k), '=', toString(v))), "
    "tags.key, tags.value)"
)
