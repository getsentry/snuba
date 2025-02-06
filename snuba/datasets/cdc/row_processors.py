from abc import ABC, abstractmethod
from typing import Type, cast

from snuba.snapshots import SnapshotTableRow
from snuba.utils.registered_class import RegisteredClass
from snuba.writer import WriterTableRow


class CdcRowProcessor(ABC, metaclass=RegisteredClass):
    @abstractmethod
    def process(self, row: SnapshotTableRow) -> WriterTableRow:
        raise NotImplementedError

    @classmethod
    def from_kwargs(cls, **kwargs: str) -> "CdcRowProcessor":
        return cls(**kwargs)

    @classmethod
    def get_from_name(cls, name: str) -> Type["CdcRowProcessor"]:
        return cast(Type["CdcRowProcessor"], cls.class_from_name(name))

    @classmethod
    def config_key(cls) -> str:
        return cls.__name__
