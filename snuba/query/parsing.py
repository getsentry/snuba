class ParsingContext:
    def __init__(self) -> None:
        self.__alias_cache = []

    def add_alias(self, alias: str) -> None:
        self.__alias_cache.append(alias)

    def is_alias_present(self, alias: str) -> bool:
        return alias in self.__alias_cache
