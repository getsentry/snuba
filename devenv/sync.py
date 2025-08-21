import importlib.metadata
import shutil

from devenv import constants
from devenv.lib import brew, config, proc, uv


def check_minimum_version(minimum_version: str) -> bool:
    version = importlib.metadata.version("sentry-devenv")

    parsed_version = tuple(map(int, version.split(".")))
    parsed_minimum_version = tuple(map(int, minimum_version.split(".")))

    return parsed_version >= parsed_minimum_version


def main(context: dict[str, str]) -> int:
    minimum_version = "1.22.1"
    if not check_minimum_version(minimum_version):
        raise SystemExit(
            f"""
In order to use uv, devenv must be at least version {minimum_version}.

Please run the following to update your global devenv:
devenv update

Then, use it to run sync:
{constants.root}/bin/devenv sync
"""
        )

    reporoot = context["reporoot"]
    cfg = config.get_repo(reporoot)

    brew.install()

    proc.run(
        (f"{constants.homebrew_bin}/brew", "bundle"),
        cwd=reporoot,
    )

    if not shutil.which("cargo"):
        raise SystemExit("cargo not on PATH. Did you run `direnv allow`?")

    uv.install(
        cfg["uv"]["version"],
        cfg["uv"][constants.SYSTEM_MACHINE],
        cfg["uv"][f"{constants.SYSTEM_MACHINE}_sha256"],
        reporoot,
    )

    print("syncing .venv ...")
    proc.run(
        (
            f"{reporoot}/.devenv/bin/uv",
            "sync",
            "--frozen",
            "--active",
            "--no-install-package",
            "rust_snuba",
            "--inexact",
        )
    )

    print("installing pre-commit hooks ...")
    proc.run((f"{reporoot}/.venv/bin/pre-commit", "install", "--install-hooks"))

    return 0
