import os

from devenv import constants
from devenv.lib import brew, colima, config, proc, venv


def main(context: dict[str, str]) -> int:
    reporoot = context["reporoot"]

    brew.install()

    proc.run(
        (f"{constants.homebrew_bin}/brew", "bundle"),
        cwd=reporoot,
    )

    venv_dir, python_version, requirements, editable_paths, bins = venv.get(
        reporoot, "venv"
    )
    url, sha256 = config.get_python(reporoot, python_version)
    print(f"ensuring venv at {venv_dir}...")
    venv.ensure(venv_dir, python_version, url, sha256)

    print(f"syncing venv with {requirements}...")
    venv.sync(reporoot, venv_dir, requirements, editable_paths, bins)

    print("running make develop...")
    os.system("make develop")

    # start colima if it's not already running
    colima.start(reporoot)

    return 0
