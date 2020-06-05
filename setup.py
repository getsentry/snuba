from setuptools import setup, find_packages


def get_requirements(path):
    with open(path) as fp:
        return [x.strip() for x in fp.read().split("\n") if not x.startswith("#")]


setup(
    name="snuba",
    packages=find_packages(exclude=["tests"]),
    zip_safe=False,
    include_package_data=True,
    install_requires=get_requirements("requirements.txt"),
    extras_require={"dev": get_requirements("requirements-dev.txt")},
    entry_points={"console_scripts": ["snuba=snuba.cli:main"]},
)
