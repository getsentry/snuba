"""
curl -X POST "https://api.datadoghq.com/api/v2/query/timeseries" \
-H "Accept: application/json" \
-H "Content-Type: application/json" \
-H "DD-API-KEY: ${DD_API_KEY}" \
-H "DD-APPLICATION-KEY: ${DD_APP_KEY}" \
-d @- << EOF
{
  "data": {
    "attributes": {
      "formulas": [
        {
          "formula": "ewma_20(a * 100)",
          "limit": {
            "count": 10,
            "order": "desc"
          }
        }
      ],
      "from": 1675159140000,
      "interval": 60000,
      "queries": [
        {
          "data_source": "metrics",
          "query": "avg:system.load.norm.1{role:snuba-errors-tiger,*} by {role}",
          "name": "a"
        }
      ],
      "to": 1675161060000
    },
    "type": "timeseries_request"
  }
}
EOF


export from=1675159140
export to=1675161060
export query="avg:system.load.norm.1{role:snuba-errors-tiger,*} by {role}"
# Curl command
curl -X GET "https://api.datadoghq.com/api/v1/query?from=${from}&to=${to}&query=${query}" \
-H "Accept: application/json" \
-H "DD-API-KEY: ${DD_API_KEY}" \
-H "DD-APPLICATION-KEY: ${DD_APP_KEY}"

"""
import json
from datetime import datetime, timezone

import requests

API_URL = "https://api.datadoghq.com/api/v2/query/timeseries"


DD_API_KEY = open("scratch/DD_API_KEY").read().strip()
DD_APP_KEY = open("scratch/DD_APP_KEY").read().strip()

START_DATETIME = datetime(2023, 1, 31, 2, 7, tzinfo=timezone.utc).timestamp()
END_DATETIME = datetime(2023, 1, 31, 2, 16, tzinfo=timezone.utc).timestamp()
INTERVAL_SECS = 5
QUERY = "avg:system.load.norm.1{role:snuba-errors-tiger,*} by {role}"


payload = {
    "data": {
        "attributes": {
            "formulas": [
                {
                    "formula": "ewma_20(a * 100)",
                    "limit": {"count": 100, "order": "desc"},
                }
            ],
            "from": int(START_DATETIME * 1000),
            "interval": INTERVAL_SECS * 1000,
            "queries": [{"data_source": "metrics", "query": QUERY, "name": "a"}],
            "to": int(END_DATETIME * 1000),
        },
        "type": "timeseries_request",
    }
}

print(json.dumps(payload, indent=2))

with open("dd_results.json", "w") as f:
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
    print(json.dumps(response.json(), indent=2))
    response.raise_for_status()
    f.write(json.dumps(response.json(), indent=2))
