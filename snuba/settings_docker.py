import os
from snuba.settings_base import *  # NOQA

env = os.environ.get

DEBUG = env("DEBUG", "0").lower() in ("1", "true")

DEFAULT_BROKERS = env("DEFAULT_BROKERS", "localhost:9092").split(",")

REDIS_HOST = env("REDIS_HOST", "localhost")
REDIS_PORT = int(env("REDIS_PORT", 6379))
REDIS_PASSWORD = env("REDIS_PASSWORD")
REDIS_DB = int(env("REDIS_DB", 1))
USE_REDIS_CLUSTER = False
