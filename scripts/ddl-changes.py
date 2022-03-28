import subprocess
from shutil import ExecError

# from sys import stdin
# import snuba.cli.migrations


def _main() -> None:
    """
    This method takes the output of `git diff --name-status master snuba/migrations` and
    runs `snuba migrations run -dry-run with the proper parameters`, for a CI action
    """
    diff_result = subprocess.run(
        ["git", "diff", "--name-status", "master", "snuba/migrations"],
        stdout=subprocess.PIPE,
        text=True,
    )
    if diff_result.returncode != 0:
        raise ExecError(diff_result.stdout)
    else:
        for line in diff_result.stdout:
            (modification_type, filename) = line.split()
            print(modification_type)
            print(filename)


if __name__ == "__main__":
    _main()
