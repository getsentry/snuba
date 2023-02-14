from __future__ import annotations

import json
from datetime import datetime

import requests

API_URL = "https://api.datadoghq.com/api/v2/query/timeseries"


DD_API_KEY = open("scratch/DD_API_KEY").read().strip()
DD_APP_KEY = open("scratch/DD_APP_KEY").read().strip()

INTERVAL_SECS = 5
QUERY = "max:system.load.norm.1{role:snuba-errors-tiger,*} by {role}"


def get_normalized_load(
    start_datetime: datetime, end_datetime: datetime, outfile: str | None
):
    payload = {
        "data": {
            "attributes": {
                "formulas": [
                    {
                        "formula": "a * 100",
                        "limit": {"count": 100, "order": "desc"},
                    }
                ],
                "from": int(start_datetime.timestamp() * 1000),
                "interval": INTERVAL_SECS * 1000,
                "queries": [{"data_source": "metrics", "query": QUERY, "name": "a"}],
                "to": int(end_datetime.timestamp() * 1000),
            },
            "type": "timeseries_request",
        }
    }

    response = requests.post(
        API_URL,
        data=json.dumps(payload),
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "DD-APPLICATION-KEY": DD_APP_KEY,
            "DD-API-KEY": DD_API_KEY,
        },
    )
    response.raise_for_status()

    response_data = response.json()
    if outfile:
        with open(outfile, "w") as f:
            f.write(json.dumps(response_data, indent=2))
    return response_data
