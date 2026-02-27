import os
import shutil

from devenv.lib import brew, proc

from devenv import constants


def main(context: dict[str, str]) -> int:
    reporoot = context["reporoot"]

    brew.install()

    proc.run(
        (f"{constants.homebrew_bin}/brew", "bundle"),
        cwd=reporoot,
    )

    if not shutil.which("rustup"):
        raise SystemExit("rustup not on PATH. Did you run `direnv allow`?")

    if os.path.exists(f"{reporoot}/.devenv/bin/uv"):
        os.remove(f"{reporoot}/.devenv/bin/uv")

    if os.path.exists(f"{reporoot}/.devenv/bin/uvx"):
        os.remove(f"{reporoot}/.devenv/bin/uvx")

    if not shutil.which("uv"):
        print("\n\n\ndevenv is no longer managing uv; please run `brew install uv`.\n\n\n")
        return 1

    print("syncing .venv ...")
    proc.run(
        (
            "uv",
            "sync",
            "--frozen",
            "--active",
            # devenv sync should just quickly update dependencies, not actually
            # build rust_snuba. However it needs to be declared as a workspace
            # dependency so the way to do this is to no-install-package and inexact
            # so it doesn't get removed.
            "--no-install-package",
            "rust_snuba",
            "--inexact",
        )
    )

    print("installing pre-commit hooks ...")
    proc.run((f"{reporoot}/.venv/bin/pre-commit", "install", "--install-hooks"))

    return 0
