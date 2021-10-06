from typing import cast

from snuba.utils.serializable_exception import SerializableException


class ClickhouseError(SerializableException):
    @property
    def code(self) -> int:
        return cast(int, self.extra_data.get("code", -1))


class ClickhouseWriterError(ClickhouseError):
    @property
    def row(self) -> int:
        return cast(int, self.extra_data.get("row", -1))
