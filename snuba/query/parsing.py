from typing import List


class ParsingContext:
    """
    This class is passed around during the query parsing process
    to keep any state needed during the process itself (like the
    alias cache).
    """

    def __init__(self) -> None:
        self.__running_aliases: List[str] = []
        self.__cache: List[str] = []

    def cache_all(self):
        for alias in self.__running_aliases:
            self.__cache.append(alias)

    def add_alias(self, alias: str) -> None:
        self.__running_aliases.append(alias)

    def is_alias_present(self, alias: str) -> bool:
        return alias in self.__cache
