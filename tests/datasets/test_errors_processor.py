error = {
    "event_id": "dcb9d002cac548c795d1c9adbfc68040",
    "project": 300688,
    "release": None,
    "dist": None,
    "platform": "python",
    "message": "",
    "datetime": "2020-01-30T22:48:49.894703Z",
    "tags": [
        ["handled", "no"],
        ["level", "error"],
        ["mechanism", "excepthook"],
        ["runtime", "CPython 3.7.6"],
        ["runtime.name", "CPython"],
        ["server_name", "snuba-transactions-consumer-production-66db778687-lhbls"],
    ],
    "_relay_processed": True,
    "breadcrumbs": {
        "values": [
            {
                "category": "snuba.utils.streams.batching",
                "level": "info",
                "timestamp": 1580424519.772524,
                "data": {"asctime": "2020-01-30 22:48:39,772"},
                "message": "New partitions assigned: {Partition(topic=Topic(name='events'), index=22): 1959057800, Partition(topic=Topic(name='events'), index=23): 1294189553, Partition(topic=Topic(name='events'), index=24): 1192835511, Partition(topic=Topic(name='events'), index=25): 1208032264, Partition(topic=Topic(name='events'), index=26): 1387920774, Partition(topic=Topic(name='events'), index=27): 1180709992, Partition(topic=Topic(name='events'), index=28): 1312057099, Partition(topic=Topic(name='events'), index=29): 1736163...",
                "type": "default",
            },
            {
                "category": "snuba.utils.streams.batching",
                "level": "info",
                "timestamp": 1580424529.867766,
                "data": {"asctime": "2020-01-30 22:48:49,867"},
                "message": "Flushing 286 items (from {Partition(topic=Topic(name='events'), index=22): Offsets(lo=1959057800, hi=1959058255), Partition(topic=Topic(name='events'), index=24): Offsets(lo=1192835511, hi=1192836351), Partition(topic=Topic(name='events'), index=23): Offsets(lo=1294189553, hi=1294190232), Partition(topic=Topic(name='events'), index=25): Offsets(lo=1208032264, hi=1208033137), Partition(topic=Topic(name='events'), index=30): Offsets(lo=1446645423, hi=1446646556), Partition(topic=Topic(name='events'), index...",
                "type": "default",
            },
            {
                "category": "httplib",
                "timestamp": 1580424529.891891,
                "type": "http",
                "data": {
                    "url": "http://127.0.0.1:8123/?load_balancing=in_order&insert_distributed_sync=1&insert_allow_materialized_columns=1&query=INSERT+INTO+transactions_dist+FORMAT+JSONEachRow",
                    "status_code": 500,
                    "reason": "Internal Server Error",
                    "method": "POST",
                },
                "level": "info",
            },
        ]
    },
    "contexts": {
        "runtime": {
            "version": "3.7.6",
            "type": "runtime",
            "name": "CPython",
            "build": "3.7.6",
        }
    },
    "culprit": "snuba.clickhouse.http in write",
    "exception": {
        "values": [
            {
                "stacktrace": {
                    "frames": [
                        {
                            "function": "<module>",
                            "abs_path": "/usr/local/bin/snuba",
                            "pre_context": [
                                "from pkg_resources import load_entry_point",
                                "",
                                "if __name__ == '__main__':",
                                "    sys.argv[0] = re.sub(r'(-script\\.pyw?|\\.exe)?$', '', sys.argv[0])",
                                "    sys.exit(",
                            ],
                            "post_context": ["    )"],
                            "vars": {
                                "__spec__": "None",
                                "__builtins__": "<module 'builtins' (built-in)>",
                                "__annotations__": {},
                                "__file__": "'/usr/local/bin/snuba'",
                                "__loader__": "<_frozen_importlib_external.SourceFileLoader object at 0x7fbbc3a36ed0>",
                                "__requires__": "'snuba'",
                                "__cached__": "None",
                                "__name__": "'__main__'",
                                "__package__": "None",
                                "__doc__": "None",
                            },
                            "module": "__main__",
                            "filename": "snuba",
                            "lineno": 11,
                            "in_app": False,
                            "data": {"orig_in_app": 1},
                            "context_line": "        load_entry_point('snuba', 'console_scripts', 'snuba')()",
                        },
                    ]
                },
                "type": "ClickHouseError",
                "module": "snuba.clickhouse.http",
                "value": "[171] DB::Exception: Block structure mismatch in RemoteBlockOutputStream stream: different types:",
                "mechanism": {"type": "excepthook", "handled": False},
            }
        ]
    },
    "extra": {
        "sys.argv": [
            "/usr/local/bin/snuba",
            "consumer",
            "--dataset",
            "transactions",
            "--consumer-group",
            "snuba-transactions-consumers",
            "--max-batch-time-ms",
            "10000",
            "--max-batch-size",
            "8192",
        ]
    },
    "fingerprint": ["{{ default }}"],
    "grouping_config": {
        "enhancements": "eJybzDhxY05qemJypZWRgaGlroGxrqHRBABbEwcC",
        "id": "legacy:2019-03-12",
    },
    "hashes": ["c8b21c571231e989060b9110a2ade7d3"],
    "key_id": "537125",
    "level": "error",
    "location": "snuba/clickhouse/http.py",
    "logger": "",
    "metadata": {
        "function": "write",
        "type": "ClickHouseError",
        "value": "[171] DB::Exception: Block structure mismatch in RemoteBlockOutputStream stream: different types:",
        "filename": "snuba/clickhouse/http.py",
    },
    "modules": {"cffi": "1.13.2", "ipython-genutils": "0.2.0", "isodate": "0.6.0",},
    "received": 1580424529.947908,
    "sdk": {
        "version": "0.13.5",
        "name": "sentry.python",
        "packages": [{"version": "0.13.5", "name": "pypi:sentry-sdk"}],
        "integrations": [
            "argv",
            "atexit",
            "dedupe",
            "excepthook",
            "logging",
            "modules",
            "stdlib",
            "threading",
        ],
    },
    "timestamp": 1580424529.894703,
    "title": "ClickHouseError: [171] DB::Exception: Block structure mismatch in RemoteBlockOutputStream stream: different types:",
    "type": "error",
    "version": "7",
}
