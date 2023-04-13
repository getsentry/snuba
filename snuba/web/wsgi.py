import time

from snuba.core.initialize import initialize_snuba
from snuba.environment import setup_logging, setup_sentry

setup_logging()
setup_sentry()
initialize_snuba()
print("sleepin")
time.sleep(100)

from snuba.web.views import application  # noqa
