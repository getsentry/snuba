from snuba.query.query_settings import HTTPQuerySettings
from snuba.request.schema import RequestSchema


def test_split_request():
    payload = {
        "turbo": False,
        "consistent": False,
        "debug": False,
        "dry_run": False,
        "legacy": False,
        "team": "sns",
        "feature": "attribution",
        "app_id": "foobar",
        "query": """MATCH (something) dontcare""",
    }
    schema = RequestSchema.build(HTTPQuerySettings)
    parts = schema.validate(payload)
    assert set(parts.query_settings.keys()) == {
        "turbo",
        "consistent",
        "debug",
        "dry_run",
        "legacy",
    }
    assert set(parts.attribution_info.keys()) == {
        "team",
        "feature",
        "app_id",
        "parent_api",
    }
    assert set(parts.query.keys()) == {"query"}
