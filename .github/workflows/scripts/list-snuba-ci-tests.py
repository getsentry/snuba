#!/usr/bin/env python3
"""Write the sentry test files that use the snuba_ci marker to OUTPUT, one per line.

With only `-m snuba_ci`, every sentry shard collects sentry's whole suite to
run the few tests that have the marker. As SELECTED_TESTS_FILE, this list makes
sentry's pytest-selected.py split just these files across the shards.

Usage, from the sentry checkout: list-snuba-ci-tests.py OUTPUT
"""

import subprocess
import sys


def main(output: str) -> int:
    grep = subprocess.run(
        ["git", "grep", "-l", r"mark\.snuba_ci", "--", "tests/*.py"],
        capture_output=True,
        text=True,
    )
    # git grep exits 1 when nothing matches and 2 or more on errors.
    if grep.returncode > 1:
        sys.stderr.write(grep.stderr)
        return grep.returncode
    files = grep.stdout.splitlines()
    if not files:
        # An empty SELECTED_TESTS_FILE would make every shard collect everything.
        print("no sentry test file uses the snuba_ci marker", file=sys.stderr)
        return 1
    with open(output, "w") as f:
        f.writelines(f"{path}\n" for path in files)
    print(f"{len(files)} sentry test files use the snuba_ci marker:", *files, sep="\n  ")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1]))
