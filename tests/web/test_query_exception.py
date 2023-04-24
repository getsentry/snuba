import json

from snuba.utils.serializable_exception import SerializableException
from snuba.web import QueryException


def test_printable() -> None:
    e = QueryException.from_args(
        "generic exception type",
        "the cause was coming from inside the house!",
        {"stats": {}, "sql": "fdsfsdaf", "experiments": {}},
    )
    assert isinstance(repr(e), str)
    assert isinstance(e.extra, dict)
    json_exc = json.dumps(e.to_dict())
    dict_exc = json.loads(json_exc)
    assert isinstance(SerializableException.from_dict(dict_exc), QueryException)
