from devenv import constants
from devenv.lib import brew, colima, config, limactl, proc, venv


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

    venv_dir, python_version, requirements, editable_paths, bins = venv.get(
        reporoot, "venv"
    )
    url, sha256 = config.get_python(reporoot, python_version)
    print(f"ensuring venv at {venv_dir}...")
    venv.ensure(venv_dir, python_version, url, sha256)

    print(f"syncing venv with {requirements}...")
    venv.sync(reporoot, venv_dir, requirements, editable_paths, bins)

    # TODO: make install-python-dependencies install-rs-dev setup-git

    # start colima if it's not already running
    colima.start(reporoot)

    return 0
