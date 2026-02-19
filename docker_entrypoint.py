#!/usr/bin/env python3
import os
import subprocess
import sys
from datetime import datetime


def main() -> None:
    args = sys.argv[1:]
    if not args:
        args = ["api"]

    if args[0].startswith("-"):
        args = ["snuba"] + args
    else:
        try:
            result = subprocess.run(
                ["snuba", args[0], "--help"],
                capture_output=True,
                timeout=30,
            )
            if result.returncode == 0:
                args = ["snuba"] + args
            else:
                print(
                    f"Error running snuba {args[0]} --help, passing command to exec directly.",
                    file=sys.stderr,
                )
                print(result.stdout.decode(errors="replace"), file=sys.stderr)
        except Exception:
            pass

    if os.environ.get("ENABLE_HEAPTRACK"):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        args = ["heaptrack", "-o", f"./profiler_data/profile_{timestamp}"] + args

    os.execvp(args[0], args)


if __name__ == "__main__":
    main()
