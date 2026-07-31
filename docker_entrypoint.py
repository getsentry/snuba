#!/usr/bin/env python3
import os
import sys
from datetime import datetime

CLI_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "snuba", "cli")


def _is_snuba_subcommand(name: str) -> bool:
    # Snuba's CLI auto-discovers commands from files in snuba/cli/, with
    # underscores in filenames replaced by dashes in the command name
    # (see SnubaCLI in snuba/cli/__init__.py). Mirror that lookup here so
    # we can detect subcommands without spawning a `snuba <cmd> --help`
    # probe — that probe runs initialize_snuba() inside get_command and
    # routinely takes 7s+, with a hard 30s timeout that occasionally
    # fires under load. When the timeout fired, the previous code swallowed
    # the exception and execvp'd args[0] directly, which then failed with
    # FileNotFoundError because subcommand names like "api" or
    # "rust-consumer" aren't binaries.
    filename = name.replace("-", "_") + ".py"
    try:
        return filename in os.listdir(CLI_DIR)
    except OSError:
        return False


def main() -> None:
    args = sys.argv[1:]
    if not args:
        args = ["api"]

    if args[0].startswith("-") or _is_snuba_subcommand(args[0]):
        args = ["snuba"] + args

    if os.environ.get("ENABLE_HEAPTRACK"):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        args = ["heaptrack", "-o", f"./profiler_data/profile_{timestamp}"] + args

    os.execvp(args[0], args)


if __name__ == "__main__":
    main()
