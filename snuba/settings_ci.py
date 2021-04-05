import os

from snuba.settings_test import *  # NOQA

env = os.environ.get

USE_REDIS_CLUSTER = env("USE_REDIS_CLUSTER", "1") != "0"
REDIS_DB = 0
REDIS_PORT = int(env("REDIS_PORT", "7000"))

ERRORS_ROLLOUT_ALL = False
ERRORS_ROLLOUT_WRITABLE_STORAGE = False
