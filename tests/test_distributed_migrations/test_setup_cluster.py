import importlib

from snuba import settings
from snuba.clusters import cluster
from snuba.migrations import runner
from snuba.settings import settings_test_distributed_migrations


def setup_function() -> None:
    settings.CLUSTERS = settings_test_distributed_migrations.CLUSTERS

    importlib.reload(cluster)
    importlib.reload(runner)
