from setuptools import setup, find_packages


def get_requirements(env):
    with open(u'requirements{}.txt'.format(env)) as fp:
        return [x.strip() for x in fp.read().split('\n') if not x.startswith('#')]


setup(
    name='snuba',
    packages=find_packages(exclude=['tests']),
    zip_safe=False,
    include_package_data=True,
    install_requires=get_requirements(""),
    extra_requires={
        ':python_version < "3.0"': get_requirements("-python2"),
        ':python_version >= "3.0"': get_requirements("-python3"),
    },
    entry_points={
        'console_scripts': [
            'snuba=snuba.cli:main',
        ],
    },
)
