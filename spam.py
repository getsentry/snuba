import requests
import json
from datetime import datetime, timedelta

start = datetime.now() - timedelta(days=1)
end = datetime.now()

url = "http://localhost:1218/transactions/snql"
def gen_payload(project_id):
    return json.dumps(
        {
            "query": f"""MATCH (transactions)
            SELECT count() AS `count`
            WHERE timestamp >= toDateTime('{start.isoformat()}')
            AND timestamp < toDateTime('{end.isoformat()}')
            AND project_id IN tuple({project_id})
            AND finish_ts >= toDateTime('{start.isoformat()}')
            AND finish_ts < toDateTime('{end.isoformat()}')
            LIMIT 1 BY count""",
            "tenant_ids": {"referrer": "r", "organization_id": 123},
        }
    )


response = requests.post(url, data=gen_payload("2"))
response.raise_for_status()
