import os

from snuba.settings.settings_test_distributed import *  # NOQA

env = os.environ.get

USE_REDIS_CLUSTER = env("USE_REDIS_CLUSTER", "1") != "0"
REDIS_DB = 0
