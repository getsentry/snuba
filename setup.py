from typing import Sequence
from setuptools import setup, find_packages


VERSION = "21.3.0.dev0"


def get_requirements() -> Sequence[str]:
    with open(u"requirements.txt") as fp:
        return [x.strip() for x in fp.read().split("\n") if not x.startswith("#")]


setup(
    name="snuba",
    packages=find_packages(exclude=["tests"]),
    zip_safe=False,
    include_package_data=True,
    install_requires=get_requirements(),
    entry_points={"console_scripts": ["snuba=snuba.cli:main"]},
)
