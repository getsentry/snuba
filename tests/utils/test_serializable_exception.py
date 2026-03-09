from sentry_sdk.utils import single_exception_from_error_tuple

from snuba.utils.serializable_exception import SerializableException


class MyException(SerializableException):
    pass


class MySubException(MyException):
    pass


exc_message = "I am an exception beep boop"


def test_exception() -> None:
    try:
        raise MyException(exc_message, kwarg1="value1", kwarg2="value2")
    except MyException as e:
        assert e.message == exc_message
        assert e.extra_data["kwarg1"] == "value1"
        assert e.extra_data["kwarg2"] == "value2"
        edict = e.to_dict()
        resurfaced_e = SerializableException.from_dict(edict)
        assert isinstance(resurfaced_e, MyException)
        assert resurfaced_e.message == exc_message
        assert resurfaced_e.extra_data == {"kwarg1": "value1", "kwarg2": "value2"}
        assert resurfaced_e.should_report


def test_from_standard_exception() -> None:
    snubs_exc = None
    # We can create snuba exceptions from standard ones
    try:
        raise Exception(exc_message)
    except Exception as e:
        snubs_exc = SerializableException.from_standard_exception_instance(e)
        assert isinstance(snubs_exc, SerializableException)
        assert snubs_exc.__class__.__name__ == "Exception"
        assert snubs_exc.message == exc_message
        assert snubs_exc.extra_data["from_standard_exception"]
        assert snubs_exc.should_report

    # if we create a SerializableException from a standard exception
    # before, we'll create the same SerializableException subclass again
    try:
        raise Exception("message is unrelated to created type")
    except Exception as e:
        new_snubs_exc = SerializableException.from_standard_exception_instance(e)
        assert isinstance(new_snubs_exc, snubs_exc.__class__)


def test_subclass_deserialization() -> None:
    deserialized_sub_exception = SerializableException.from_dict(MySubException().to_dict())
    assert isinstance(deserialized_sub_exception, MyException)


def test_sentry_integration() -> None:
    # tests that when the exception is captured by sentry,
    # the exception message is not swallowed by deserialization
    ex = MySubException("message")
    try:
        raise ex
    except SerializableException as e:
        assert single_exception_from_error_tuple(MySubException, e, tb=None)["value"] == "message"
    try:
        raise SerializableException.from_dict(ex.to_dict())
    except SerializableException as e2:
        assert single_exception_from_error_tuple(MySubException, e2, tb=None)["value"] == "message"
