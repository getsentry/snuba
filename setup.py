import sys
from setuptools import setup, find_packages

install_requires = []

with open('requirements-py%d.txt' % sys.version_info[0]) as fp:
    for l in fp:
        l = l.strip()
        if not l or l[0] == '#':
            continue
        install_requires.append(l)

setup(
    name='snuba',
    packages=find_packages(exclude=['tests']),
    zip_safe=False,
    install_requires=install_requires,
    dependency_links=[
        'git+https://github.com/getsentry/sentry-python.git#egg=sentry_sdk',
    ],
    entry_points={
        'console_scripts': [
            'snuba=snuba.cli:main',
        ],
    },
)
