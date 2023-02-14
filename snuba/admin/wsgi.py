from snuba.environment import setup_logging, setup_sentry

setup_logging()
setup_sentry()

from snuba.core.initialize import initialize_snuba

initialize_snuba()
from snuba.admin.views import application  # noqa
