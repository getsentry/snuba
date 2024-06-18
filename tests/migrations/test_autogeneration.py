import os
import subprocess

from snuba.migrations.autogeneration import generate


def test_basic() -> None:
    dir = "/tmp/kylesfakerepo987636"
    fname = "fakestorage.yaml"

    # make a tmp dir with a git repo
    if os.path.exists(dir):
        subprocess.run(["rm", "-rf", dir], check=True)
    os.makedirs(dir)
    try:
        subprocess.run(["git", "init"], cwd=dir, check=True)
        # make a fake storage
        with open(os.path.join(dir, fname), "w") as f:
            f.write("hello world\n")
        subprocess.run(
            ["git", "add", "."],
            cwd=dir,
            check=True,
        )
        subprocess.run(
            ["git", "commit", "-m", '"blop"'],
            cwd=dir,
            check=True,
        )
    except subprocess.CalledProcessError as e:
        if not e.stderr:
            raise
        raise ValueError(e.stderr.decode("utf-8")) from e

    # update the fake storage
    with open(os.path.join(dir, fname), "a") as f:
        f.write("goodbye world")

    # make sure HEAD and curr version looks right
    old_storage, new_storage = generate(os.path.join(dir, fname))
    assert old_storage == "hello world\n"
    assert new_storage == "hello world\ngoodbye world"
