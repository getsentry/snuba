#!/usr/bin/env python3

import datetime
import json
import uuid

import requests

# Generate data for uptime checks for each minute of a day
data = []
base_time = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

for minute in range(24 * 60 * 90):  # 24 hours * 60 minutes * 90 days
    timestamp = base_time + datetime.timedelta(minutes=minute)
    data.append(
        {
            "project_id": 555,
            "environment_id": 100,
            "uptime_subscription_id": 5678,
            "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "uptime_check_id": str(uuid.uuid4()),
            "duration": 150,
            "location_id": 1,
            "status": 200,  # All checks are successful
            "timeout_at": 30,
            "trace_id": str(uuid.uuid4()),
        }
    )

# Insert data into ClickHouse using HTTP API
query = """
INSERT INTO default.uptime_monitors_local (
    project_id, environment_id, uptime_subscription_id, timestamp,
    uptime_check_id, duration, location_id, status, timeout_at, trace_id
) FORMAT JSONEachRow
"""

response = requests.post(
    "http://localhost:8123",
    params={"query": query},
    data="\n".join(json.dumps(row) for row in data),
)

if response.status_code == 200:
    print(f"Successfully inserted {len(data)} records into uptime_monitors_local")
else:
    print(f"Error inserting data: {response.text}")
