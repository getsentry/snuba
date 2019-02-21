import os
from snuba.settings_base import *

env = os.environ.get

DEBUG = env('DEBUG', '0').lower() in ('1', 'true')

CLICKHOUSE_SERVER = env('CLICKHOUSE_SERVER', 'localhost:9000')
CLICKHOUSE_TABLE = env('CLICKHOUSE_TABLE', 'sentry')
DEFAULT_DIST_TABLE = CLICKHOUSE_TABLE
DEFAULT_LOCAL_TABLE = CLICKHOUSE_TABLE


DEFAULT_BROKERS = env('DEFAULT_BROKERS', 'localhost:9092').split(',')

REDIS_HOST = env('REDIS_HOST', 'localhost')
REDIS_PORT = int(env('REDIS_PORT', 6379))
REDIS_DB = int(env('REDIS_DB', 1))
USE_REDIS_CLUSTER = False
