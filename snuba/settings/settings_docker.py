import os

env = os.environ.get

DEBUG = env("DEBUG", "0").lower() in ("1", "true")

DEFAULT_RETENTION_DAYS = env("SENTRY_EVENT_RETENTION_DAYS", 90)

REDIS_HOST = env("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(env("REDIS_PORT", 6379))
REDIS_PASSWORD = env("REDIS_PASSWORD")
REDIS_DB = int(env("REDIS_DB", 1))
USE_REDIS_CLUSTER = False

# Dogstatsd Options
SNUBA_STATSD_ADDR = env("SNUBA_STATSD_ADDR")
if SNUBA_STATSD_ADDR:
    ip, separator, port = SNUBA_STATSD_ADDR.rpartition(':')
    DOGSTATSD_HOST = ip
    DOGSTATSD_PORT = port
else:
    DOGSTATSD_HOST = env("DOGSTATSD_HOST")
    DOGSTATSD_PORT = env("DOGSTATSD_PORT")

SENTRY_DSN = env("SENTRY_DSN")

USE_EAP_ITEMS_TABLE = True
