ALIAS_PREFIX = "_snuba_"


def aliasify_column(col_name: str) -> str:
    return f"{ALIAS_PREFIX}{col_name}"
