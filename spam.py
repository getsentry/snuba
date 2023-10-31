import requests
import json
from datetime import datetime, timedelta
from multiprocessing import Pool
import random
import time
import itertools

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

num_projects = 100
queries_per_project = 1000

def query_api(project_id):
    response = requests.post(url, data=gen_payload(project_id))
    if random.random() < 0.01 or response.status_code != 200:
        print("random_request", project_id, response.status_code)




def main():
    queries = []
    for project_id in range(1, num_projects + 1):
        queries.extend([project_id] * queries_per_project)

    random.shuffle(queries)

    Pool(10).map(query_api, queries)

if __name__ == "__main__":
    main()


