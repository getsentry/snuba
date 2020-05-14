from typing import Dict, Optional


class ParsingContext:
    """
    This class is passed around during the query parsing process
    to keep any state needed during the process itself (like the
    alias cache).
    """

    def __init__(self) -> None:
        self.__alias_cache: Dict[str, str] = {}

    def add_alias(self, alias: str, expression: str) -> None:
        self.__alias_cache[alias] = expression

    def is_alias_present(self, alias: str) -> bool:
        return alias in self.__alias_cache

    def get_expression_for_alias(self, alias: str) -> Optional[str]:
        return self.__alias_cache.get(alias)
