from snuba.utils.snuba_exception import SnubaException


class MyException(SnubaException):
    pass


class MySubException(MyException):
    pass


exc_message = "I am an exception beep boop"


def test_exception() -> None:
    try:
        raise MyException(exc_message, extra="data")
    except MyException as e:
        assert e.message == exc_message
        assert e.extra_data["extra"] == "data"
        edict = e.to_dict()
        resurfaced_e = SnubaException.from_dict(edict)
        assert isinstance(resurfaced_e, MyException)
        assert resurfaced_e.message == exc_message
        assert resurfaced_e.extra_data == {"extra": "data"}
        assert resurfaced_e.should_report


def test_from_standard_exception() -> None:
    snubs_exc = None
    # We can create snuba exceptions from standard ones
    try:
        raise Exception(exc_message)
    except Exception as e:
        snubs_exc = SnubaException.from_standard_exception_instance(e)
        assert isinstance(snubs_exc, SnubaException)
        assert snubs_exc.__class__.__name__ == "Exception"
        assert snubs_exc.message == exc_message
        assert snubs_exc.extra_data["from_standard_exception"]
        assert snubs_exc.should_report

    # if we create a SnubaException from a standard exception
    # before, we'll create the same SnubaException subclass again
    try:
        raise Exception("message is unrelated to created type")
    except Exception as e:
        new_snubs_exc = SnubaException.from_standard_exception_instance(e)
        assert isinstance(new_snubs_exc, snubs_exc.__class__)


def test_subclass_deserialization() -> None:
    deserialized_sub_exception = SnubaException.from_dict(MySubException().to_dict())
    assert isinstance(deserialized_sub_exception, MyException)
