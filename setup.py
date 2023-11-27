from typing import Sequence

from setuptools import find_packages, setup

VERSION = "23.11.2"


def get_requirements() -> Sequence[str]:
    with open("requirements.txt") as fp:
        return [
            x.strip() for x in fp.read().split("\n") if not x.startswith(("#", "--"))
        ]


setup(
    name="snuba",
    version=VERSION,
    packages=find_packages(exclude=["tests"]),
    zip_safe=False,
    include_package_data=True,
    install_requires=get_requirements(),
    entry_points={"console_scripts": ["snuba=snuba.cli:main"]},
)
