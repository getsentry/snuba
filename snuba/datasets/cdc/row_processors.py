from abc import ABC, abstractmethod
from typing import Type, cast

from snuba.datasets.cdc.groupassignee_processor import GroupAssigneeRow
from snuba.datasets.cdc.groupedmessage_processor import GroupedMessageRow
from snuba.snapshots import SnapshotTableRow
from snuba.utils.registered_class import RegisteredClass
from snuba.writer import WriterTableRow


class CdcRowProcessor(ABC, metaclass=RegisteredClass):
    @abstractmethod
    def process(self, row: SnapshotTableRow) -> WriterTableRow:
        raise NotImplementedError

    @classmethod
    def get_from_name(cls, name: str) -> Type["CdcRowProcessor"]:
        return cast(Type["CdcRowProcessor"], cls.class_from_name(name))

    @classmethod
    def config_key(cls) -> str:
        return cls.__name__


class GroupAssigneeRowProcessor(CdcRowProcessor):
    def process(self, row: SnapshotTableRow) -> WriterTableRow:
        return GroupAssigneeRow.from_bulk(row).to_clickhouse()


class GroupedMessageRowProcessor(CdcRowProcessor):
    def process(self, row: SnapshotTableRow) -> WriterTableRow:
        return GroupedMessageRow.from_bulk(row).to_clickhouse()
