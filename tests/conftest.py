from snuba.environment import setup_sentry


def pytest_configure():
    """ Set up the Sentry SDK to avoid errors hidden by configuration """
    setup_sentry()
