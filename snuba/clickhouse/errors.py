from typing import cast

from snuba.utils.serializable_exception import SerializableException


class ClickhouseError(SerializableException):
    def format_message(self, message: str) -> str:
        msg = message
        if "while processing query:" in message:
            msg = message.split("while processing query:")[0].strip()
        if "Stack trace:" in message:
            msg = message.split("Stack trace:")[0].strip()

        return 'Code: {}. {}"'.format(self.code, msg)

    @property
    def code(self) -> int:
        return cast(int, self.extra_data.get("code", -1))


class ClickhouseWriterError(ClickhouseError):
    @property
    def row(self) -> int:
        return cast(int, self.extra_data.get("row", -1))
