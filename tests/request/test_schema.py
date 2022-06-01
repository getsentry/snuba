from snuba.query.query_settings import HTTPQuerySettings, SubscriptionQuerySettings
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
    import pdb

    pdb.set_trace()
    print(parts)
