from setuptools import setup, find_packages


setup(
    name='snuba',
    packages=find_packages(exclude=['tests']),
    zip_safe=False,
    include_package_data=True,
    install_requires=[
        'appnope==0.1.0',
        'argh==0.26.2',
        'astroid==1.6.5',
        'atomicwrites==1.1.5',
        'attrs==18.1.0',
        'autopep8==1.3.5',
        'backcall==0.1.0',
        'batching-kafka-consumer==0.0.5',
        'blinker==1.4',
        'certifi==2018.4.16',
        'chardet==3.0.4',
        'click==6.7',
        'clickhouse-driver==0.0.18',
        'colorama==0.3.9',
        'configparser==3.5.0',
        'confluent-kafka==0.11.5',
        'contextlib2==0.5.5',
        'coverage==4.5.1',
        'datadog==0.21.0',
        'decorator==4.3.0',
        'deprecation==2.0.3',
        'docopt==0.6.2',
        'enum34==1.1.6',
        'flake8==3.5.0',
        'Flask==1.0.2',
        'funcsigs==1.0.2',
        'future==0.16.0',
        'honcho==1.0.1',
        'idna==2.7',
        'ipdb==0.11',
        'ipython==6.4.0',
        'ipython-genutils==0.2.0',
        'isodate==0.6.0',
        'isort==4.3.4',
        'itsdangerous==0.24',
        'jedi==0.12.0',
        'Jinja2==2.10',
        'jsonschema==3.0.1',
        'lazy-object-proxy==1.3.1',
        'linecache2==1.0.0',
        'lz4==2.0.0',
        'Markdown==2.6.11',
        'MarkupSafe==1.0',
        'mccabe==0.6.1',
        'mock==2.0.0',
        'more-itertools==4.2.0',
        'packaging==17.1',
        'parso==0.2.1',
        'pathlib2==2.3.2',
        'pathtools==0.1.2',
        'pbr==4.0.4',
        'petname==2.2',
        'pexpect==4.6.0',
        'pickleshare==0.7.4',
        'pluggy==0.7.1',
        'prompt-toolkit==1.0.15',
        'ptyprocess==0.5.2',
        'py==1.5.3',
        'pycodestyle==2.3.1',
        'pyflakes==1.6.0',
        'Pygments==2.2.0',
        'pylint==1.9.2',
        'pyparsing==2.2.0',
        'pytest==3.9.3',
        'pytest-cov==2.5.1',
        'pytest-watch==4.2.0',
        'python-dateutil==2.7.3',
        'pytz==2018.4',
        'PyYAML==3.13',
        'redis==2.10.6',
        'redis-py-cluster==1.3.5',
        'sentry-sdk==0.10.0',
        'simplegeneric==0.8.1',
        'simplejson==3.15.0',
        'singledispatch==3.4.0.3',
        'traceback2==1.4.0',
        'traitlets==4.3.2',
        'unittest2==1.1.0',
        'urllib3==1.25.3',
        'uWSGI==2.0.17',
        'watchdog==0.8.3',
        'wcwidth==0.1.7',
        'Werkzeug==0.14.1',
        'wrapt==1.10.11',
    ],
    entry_points={
        'console_scripts': [
            'snuba=snuba.cli:main',
        ],
    },
)
