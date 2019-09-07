from __future__ import annotations

from typing import Any, Mapping, Sequence


class Query:
    """
    Represents a parsed query we can edit during query processing.

    This is the bare minimum step in this direction. This is a bare
    minimum wrapper around the query representation coming in from the
    client, but it allows us to limit the operaitons we want to expose
    to the query processing logic so that replacing it with a proper
    abstract query structure will be much easier.
    """

    def __init__(self, body: Mapping[str, Any]):
        self.__body = body

    def get_body(self) -> Mapping[str, Any]:
        return self.__body

    def add_conditions(
        self,
        conditions: Sequence[Any],
    ) -> None:
        self.__body["conditions"].extend(conditions)

    def get_conditions(self) -> Sequence[Any]:
        return self.__body["conditions"]

    def set_field(
        self,
        field: str,
        value: Any,
    ) -> None:
        self.__body[field] = value
