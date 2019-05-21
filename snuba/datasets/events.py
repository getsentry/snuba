from snuba.datasets import DataSet
from snuba.datasets.schema import TableSchema


class EventsDataSet(DataSet):
    """
    Represents the collection of classic sentry "error" type events
    and the particular quirks of storing and querying them.
    """

    SCHEMA = TableSchema(
        local_table_name='sentry_local',
        dist_table_name='sentry_dist')

    def default_conditions(self, body):
        return [
            ('deleted', '=', 0),
        ]
