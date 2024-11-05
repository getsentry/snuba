from devenv import constants
from devenv.lib import brew, colima, config, limactl, proc


def main(context: dict[str, str]) -> int:
    reporoot = context["reporoot"]
    cfg = config.get_repo(reporoot)

    brew.install()

    proc.run(
        (f"{constants.homebrew_bin}/brew", "bundle"),
        cwd=reporoot,
    )

    colima.install(
        cfg["colima"]["version"],
        cfg["colima"][constants.SYSTEM_MACHINE],
        cfg["colima"][f"{constants.SYSTEM_MACHINE}_sha256"],
        reporoot,
    )
    limactl.install(
        cfg["lima"]["version"],
        cfg["lima"][constants.SYSTEM_MACHINE],
        cfg["lima"][f"{constants.SYSTEM_MACHINE}_sha256"],
        reporoot,
    )

    # start colima if it's not already running
    colima.start(reporoot)

    return 0
