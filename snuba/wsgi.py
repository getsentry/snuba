def configure() -> None:
    import logging
    import os

    from snuba import settings

    logging.basicConfig(
        level=getattr(logging, settings.LOG_LEVEL.upper()),
        format="%(asctime)s %(message)s",
    )

    import sentry_sdk
    from sentry_sdk.integrations.flask import FlaskIntegration
    from sentry_sdk.integrations.gnu_backtrace import GnuBacktraceIntegration

    sentry_sdk.init(
        dsn=settings.SENTRY_DSN,
        integrations=[FlaskIntegration(), GnuBacktraceIntegration()],
        release=os.getenv("SNUBA_RELEASE"),
    )


configure()

from snuba.api.views import application  # noqa
