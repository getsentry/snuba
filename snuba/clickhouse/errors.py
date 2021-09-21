from typing import cast

from snuba.utils.snuba_exception import SnubaException


class ClickhouseError(SnubaException):
    @property
    def code(self) -> int:
        return cast(int, self.extra_data.get("code", -1))


class ClickhouseWriterError(ClickhouseError):
    @property
    def row(self) -> int:
        return cast(int, self.extra_data.get("row", -1))
