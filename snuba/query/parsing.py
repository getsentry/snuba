class ParsingContext:
    """
    This class is passed through during the query parsing process
    to keep any state needed during the process itself (like the
    alias cache).
    """

    def __init__(self) -> None:
        self.__alias_cache = []

    def add_alias(self, alias: str) -> None:
        self.__alias_cache.append(alias)

    def is_alias_present(self, alias: str) -> bool:
        return alias in self.__alias_cache
