import os

from devenv import constants
from devenv.lib import brew, colima, config, proc, uv


def main(context: dict[str, str]) -> int:
    reporoot = context["reporoot"]
    cfg = config.get_repo(reporoot)

    brew.install()

    proc.run(
        (f"{constants.homebrew_bin}/brew", "bundle"),
        cwd=reporoot,
    )

    uv.install(
        cfg["uv"]["version"],
        cfg["uv"][constants.SYSTEM_MACHINE],
        cfg["uv"][f"{constants.SYSTEM_MACHINE}_sha256"],
        reporoot,
    )

    print(f"syncing .venv ...")
    proc.run(("uv", "sync", "--frozen", "--quiet"))

    print("running make install-rs-dev...")
    os.system("make install-rs-dev")

    # start colima if it's not already running
    colima.start(reporoot)

    return 0
