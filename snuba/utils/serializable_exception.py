"""
SerializableException: the base class for all custom exceptions in the snuba project which
allows for serialization, deserialization, and re-raising the same exception from the
deserialized version.

Diagram:

┌──────────────────────────────────┐                           ┌──────────────────────────────────────────────┐
│          MachineA                │                           │         MachineB                             │
│                                  │MyException().to_dict()    │class MyException(                            │
│  class MyException(              ├──────────────────────────►│   SerializableException)                     │
│       SerializableException)     │                           │recvd_exc = SnubaException.from_dict(payload) │
│                                  │                           │assert isinstance(recvd_exc, MyException)     │
└──────────────────────────────────┘                           └──────────────────────────────────────────────┘

Usage:

>>> # Sender code
>>> from snuba.utils.serializable_exception import SerializableException
>>>
>>> class MyException(SerializableException):
>>>     pass
>>>
>>> try:
>>>     raise MyException(
>>>         message="this is a message",
>>>         should_report=False # this should not be reported to sentry
>>>     )
>>> except SerializableException as e:
>>>     # serialize it
>>>     send_somewhere(rapidjson.dumps(e.to_dict()))

# Receiver code

>>> from snuba.utils.serializable_exception import SerializableException
>>> # Both sender AND receiver have to define the exception with the same
>>> # name to be able to resurface the exception
>>> class MyException(SerializableException):
>>>     pass
>>>
>>> recvd_exception_dict = rapidjson.loads(recv())
>>> raise SerializableException.from_dict(recvd_exception_dict) # this will be an instance of MyException
"""

from typing import Any, Dict, List, Optional, TypedDict, Union, cast

import rapidjson

from snuba.utils.registered_class import RegisteredClass

# mypy has not figured out recursive types yet so this can't be totally typesafe
JsonSerializable = Union[str, int, float, bool, None, Dict[str, Any], List[Any]]


class SerializableExceptionDict(TypedDict):
    __type__: str
    __name__: str
    __message__: str
    __extra_data__: Dict[str, JsonSerializable]
    __should_report__: bool


class SerializableException(Exception, metaclass=RegisteredClass):
    def __init__(
        self,
        message: Optional[str] = None,
        should_report: bool = True,
        **extra_data: JsonSerializable
    ) -> None:
        self.message = message or ""
        self.extra_data = extra_data or {}
        # whether or not the error should be reported to sentry
        self.should_report = should_report
        super().__init__(message)

    def to_dict(self) -> SerializableExceptionDict:
        return {
            "__type__": "SerializableException",
            "__name__": self.__class__.__name__,
            "__message__": self.message,
            "__should_report__": self.should_report,
            "__extra_data__": self.extra_data,
        }

    @classmethod
    def from_dict(cls, edict: SerializableExceptionDict) -> "SerializableException":
        assert edict["__type__"] == "SerializableException"
        defined_exception = cls._registry.get_class_by_name(edict.get("__name__", ""))

        if defined_exception is not None:
            return defined_exception(
                message=edict.get("__message__", ""),
                should_report=edict.get("__should_report__", True),
                **edict.get("__extra_data__", {})
            )
        # if an exception is created from a dictionary which is not in the registry,
        # create a new Exception type with that name and message dynamically.
        # This allows gracefully handling the receiver not having the exception defined
        # on its end while still allowing normal exception behavior.
        return cast(
            SerializableException,
            type(edict["__name__"], (cls,), {})(
                message=edict.get("__message__", ""),
                should_report=edict.get("__should_report__", True),
                **edict.get("__extra_data__", {})
            ),
        )

    @classmethod
    def from_standard_exception_instance(
        cls, exc: Exception
    ) -> "SerializableException":
        if isinstance(exc, cls):
            return exc
        return cls.from_dict(
            {
                "__type__": "SerializableException",
                "__name__": exc.__class__.__name__,
                "__message__": str(exc),
                "__extra_data__": {"from_standard_exception": True},
                "__should_report__": True,
            }
        )

    def __repr__(self) -> str:
        return cast(str, rapidjson.dumps(self.to_dict(), indent=2))
