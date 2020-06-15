from snuba import settings
from snuba.clickhouse.native import ClickhousePool
from snuba.environment import setup_sentry


def pytest_configure() -> None:
    """
    Set up the Sentry SDK to avoid errors hidden by configuration.
    Ensure the snuba_test database exists
    """
    setup_sentry()

    # There is only one cluster in test, so fetch the host from there.
    cluster = settings.CLUSTERS[0]

    connection = ClickhousePool(
        cluster["host"], cluster["port"], "default", "", "default",
    )

    database_name = cluster["database"]
    connection.execute(f"CREATE DATABASE IF NOT EXISTS {database_name};")
