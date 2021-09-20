from snuba.utils.snuba_exception import SnubaException


def test_exception() -> None:
    class MyException(SnubaException):
        pass

    message = "I am an exception beep boop"
    try:
        raise MyException(message, extra="data")
    except MyException as e:
        assert e.message == message
        assert e.extra_data["extra"] == "data"
        edict = e.to_dict()
        resurfaced_e = SnubaException.from_dict(edict)
        assert isinstance(resurfaced_e, MyException)
        assert resurfaced_e.message == message
        assert resurfaced_e.extra_data == {"extra": "data"}


def test_from_standard_exception() -> None:
    message = "I am an exception beep boop"
    try:
        raise Exception(message)
    except Exception as e:
        snubs_exc = SnubaException.from_standard_exception_instance(e)
        assert isinstance(snubs_exc, SnubaException)
        assert snubs_exc.__class__.__name__ == "Exception"
        assert snubs_exc.message == message
