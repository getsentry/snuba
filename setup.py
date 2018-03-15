from setuptools import setup, find_packages

setup(
    name='snuba',
    packages=find_packages(exclude=['tests']),
)
