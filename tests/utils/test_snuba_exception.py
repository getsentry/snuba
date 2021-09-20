from snuba.utils.snuba_exception import SnubaException


def test_exception() -> None:
    class MyException(SnubaException):
        pass

    try:
        raise MyException("poopoo", extra="data")
    except MyException as e:
        assert e.message == "poopoo"
        assert e.extra_data["extra"] == "data"
        edict = e.to_dict()
        resurfaced_e = SnubaException.from_dict(edict)
        assert resurfaced_e.__class__.__name__ == "MyException"
        assert resurfaced_e.message == "poopoo"
        assert resurfaced_e.extra_data == {"extra": "data"}
