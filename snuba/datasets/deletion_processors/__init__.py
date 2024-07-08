from __future__ import annotations

from abc import ABC
from typing import cast

from snuba.utils.registered_class import RegisteredClass


class DeletionProcessor(ABC, metaclass=RegisteredClass):
    def __init__(
        self,
        column_filters: list[str],
    ) -> None:
        self.column_filters = column_filters

    @classmethod
    def config_key(cls) -> str:
        return cls.__name__

    @classmethod
    def get_from_name(cls, name: str) -> "DeletionProcessor":
        return cast("DeletionProcessor", cls.class_from_name(name))

    @classmethod
    def from_kwargs(cls, **kwargs: str) -> "DeletionProcessor":
        column_filters = kwargs.pop("column_filters", None)
        assert isinstance(
            column_filters, list
        ), "column_filters must be a list of strings"
        return cls(
            column_filters=column_filters,
            **kwargs,
        )


DEFAULT_DELETION_PROCESSOR = DeletionProcessor(column_filters=[])
