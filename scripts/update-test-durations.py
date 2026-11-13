#!/usr/bin/env python3
"""Refresh tests/ci_test_durations.json from CI JUnit reports.

The `tests` CI job uploads `junit-<settings>-<shard>` artifacts. Download the
shards for one or more settings modes from a recent green master run, e.g.

    gh run download RUN_ID -p 'junit-*' -D /tmp/junit

then run this script from the repository root:

    python3 scripts/update-test-durations.py /tmp/junit

Each artifact directory name gives the settings mode, and every shard of a
mode is merged. Modes not present in the input keep their current durations.
"""

from __future__ import annotations

import json
import os
import sys
import xml.etree.ElementTree as ET
from collections import defaultdict
from pathlib import Path

DURATIONS_PATH = Path("tests/ci_test_durations.json")


def test_file(classname: str) -> str | None:
    # pytest writes `tests.path.to.test_module[.TestClass]`; the file is the
    # longest dotted prefix that exists on disk.
    parts = classname.split(".")
    for end in range(len(parts), 0, -1):
        path = os.path.join(*parts[:end]) + ".py"
        if os.path.isfile(path):
            return path
    return None


def main(artifacts_dir: str) -> None:
    by_mode: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for report in sorted(Path(artifacts_dir).glob("junit-*/**/pytest.junit.xml")):
        artifact = report.relative_to(artifacts_dir).parts[0]
        mode = artifact.removeprefix("junit-").rsplit("-", 1)[0]
        for case in ET.parse(report).iter("testcase"):
            path = test_file(case.get("classname", ""))
            if path is not None:
                by_mode[mode][path] += float(case.get("time", 0))

    durations = json.loads(DURATIONS_PATH.read_text()) if DURATIONS_PATH.exists() else {}
    for mode, files in by_mode.items():
        durations[mode] = {path: round(seconds, 1) for path, seconds in sorted(files.items())}
        print(f"{mode}: {len(files)} files, {sum(files.values()):.0f}s")
    DURATIONS_PATH.write_text(json.dumps(durations, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main(sys.argv[1])
