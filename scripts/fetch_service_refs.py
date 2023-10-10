#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from typing import Any, Dict, Optional

GO_SERVER_URL = os.environ["GO_SERVER_URL"]

# the maximum number of times to fetch the pipeline history
MAX_FETCHES = 100


def pipeline_passed(pipeline: Dict[str, Any]) -> bool:
    # stage["result"] isn't populated if it isn't run
    # the other possible statuses are Unknown (not run yet), Cancelled, Failed
    return all(
        stage["status"] == "Passed"
        for stage in pipeline["stages"]
        if stage["name"] != "migrate"
    )


# print the most recent passing sha for a repo
def main(pipeline_name: str = "deploy-snuba-us", repo: str = "snuba") -> int:
    GOCD_ACCESS_TOKEN = os.environ.get("GOCD_ACCESS_TOKEN")
    if not GOCD_ACCESS_TOKEN:
        raise SystemExit(
            """
            GOCD_ACCESS_TOKEN not set. It should be an access token belonging to bot@sentry.io.
            """
        )
    fetch_url: Optional[str] = f"{GO_SERVER_URL}/api/pipelines/{pipeline_name}/history"
    fetches = 0
    while fetch_url and fetches < MAX_FETCHES:
        fetches += 1
        req = urllib.request.Request(
            fetch_url,
            headers={
                "Accept": "application/vnd.go.cd.v1+json",
                "Authorization": f"bearer {GOCD_ACCESS_TOKEN}",
            },
        )
        try:
            resp = urllib.request.urlopen(req)
        except urllib.error.HTTPError as e:
            raise SystemExit(f"Failed to fetch pipeline history:\n{e.read().decode()}")

        print(
            "fetching pipeline history for", pipeline_name, fetch_url, file=sys.stderr
        )
        data = json.loads(resp.read())

        if "_links" in data and "next" in data["_links"]:
            fetch_url = data["_links"]["next"]["href"]
        else:
            fetch_url = None
        rev = None

        for pipeline in sorted(
            data["pipelines"], key=lambda _: int(_["counter"]), reverse=True
        ):
            # Look at the most recent passing pipeline,
            # and get its deployment revision for the main material.
            if pipeline_passed(pipeline):

                for r in pipeline["build_cause"]["material_revisions"]:
                    # example material description format... `in` is good enough
                    # 'URL: git@github.com:getsentry/devinfra-example-service.git, Branch: main'
                    if (
                        f"git@github.com:getsentry/{repo}.git"
                        in r["material"]["description"]
                    ):
                        rev = r["modifications"][0]["revision"]
                        print(rev)
                        return 0
                    elif (
                        f"https://github.com/getsentry/{repo}.git"
                        in r["material"]["description"]
                    ):
                        rev = r["modifications"][0]["revision"]
                        print(rev)
                        return 0
        time.sleep(1)

    if rev is None:
        raise SystemExit(f"Couldn't find passed pipeline for {repo}")
    print(rev)
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--pipeline", default="deploy-snuba")
    parser.add_argument("--repo", default="snuba")
    args = parser.parse_args()
    main(args.pipeline, args.repo)
