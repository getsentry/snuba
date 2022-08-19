import json

from snuba.web import QueryException


def test_printable():
    e = QueryException.from_args({"stats": {}, "sql": "fdsfsdaf", "experiments": {}})
    assert isinstance(repr(e), str)
    assert isinstance(e.extra, dict)
    json.dumps(e.to_dict())
