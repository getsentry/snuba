#!/usr/bin/env python3

import datetime
import json
import random
import uuid

import requests

# Generate and insert data for uptime checks for each project
base_time = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

query = """
INSERT INTO default.uptime_monitor_checks_local (
    organization_id, project_id, environment, uptime_subscription_id, uptime_check_id,
    scheduled_check_time, timestamp, duration, region_id, check_status,
    check_status_reason, http_status_code, trace_id, retention_days
) FORMAT JSONEachRow
"""

total_records = 0

for project_id in range(1, 2):
    project_data = []
    for minute in range(24 * 60 * 90):  # 24 hours * 60 minutes * 90 days
        timestamp = base_time + datetime.timedelta(minutes=minute)
        scheduled_time = timestamp - datetime.timedelta(seconds=random.randint(1, 30))
        http_status = (
            500 if minute % 100 == 0 else 200
        )  # Every 100th record gets status 500
        check_status = "failure" if http_status == 500 else "success"
        project_data.append(
            {
                "organization_id": 1,
                "project_id": project_id,
                "environment": "production",
                "uptime_subscription_id": random.randint(1, 3) * project_id,
                "uptime_check_id": str(uuid.uuid4()),
                "scheduled_check_time": scheduled_time.strftime("%Y-%m-%d %H:%M:%S"),
                "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                "duration": random.randint(1, 1000),
                "region_id": random.randint(1, 3),
                "check_status": check_status,
                "check_status_reason": "Timeout error"
                if check_status == "failure"
                else None,
                "http_status_code": http_status,
                "trace_id": str(uuid.uuid4()),
                "retention_days": 30,
            }
        )

    response = requests.post(
        "http://localhost:8123",
        params={"query": query},
        data="\n".join(json.dumps(row) for row in project_data),
    )

    if response.status_code == 200:
        total_records += len(project_data)
        print(
            f"Successfully inserted {len(project_data)} records for project {project_id}"
        )
    else:
        print(f"Error inserting data for project {project_id}: {response.text}")

print(f"Total records inserted: {total_records}")
