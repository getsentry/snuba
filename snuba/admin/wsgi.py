from snuba.environment import setup_logging, setup_sentry

setup_logging()
setup_sentry()

from snuba.admin.views import application  # noqa
