from os import getenv
from typing import Sequence
from setuptools import setup, find_packages


VERSION = "21.4.0.dev0"


def get_requirements() -> Sequence[str]:
    requirements = []
    with open(u"requirements.txt") as fp:
        requirements = [
            x.strip() for x in fp.read().split("\n") if not x.startswith("#")
        ]

    if getenv("SNUBA_SETTINGS") in ("ci", "test"):
        with open(u"requirements-test.txt") as fp:
            requirements.extend(
                [x.strip() for x in fp.read().split("\n") if not x.startswith("#")]
            )

    return requirements


setup(
    name="snuba",
    packages=find_packages(exclude=["tests"]),
    zip_safe=False,
    include_package_data=True,
    install_requires=get_requirements(),
    entry_points={"console_scripts": ["snuba=snuba.cli:main"]},
)
