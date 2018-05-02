from snuba.settings_base import *  # NOQA

DOGSTATSD_PORT = 8126
CLICKHOUSE_SERVER = os.environ.get('CLICKHOUSE_SERVER', '127.0.0.1:9001')
