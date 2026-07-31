from snuba.environment import setup_logging, setup_sentry

setup_logging()
setup_sentry()

from snuba.core.initialize import initialize_snuba  # noqa: E402  # must run after setup_sentry

initialize_snuba()
from snuba.admin.views import application  # noqa: E402, F401  # WSGI entrypoint; import after init
