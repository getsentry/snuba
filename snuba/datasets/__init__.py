class DataSet(object):
    """
    A DataSet defines the complete set of data sources, schemas, and
    transformations that are required to:
        - Consume, transform, and insert data payloads from Kafka into Clickhouse.
        - Define how Snuba API queries are transformed into SQL.

    This is the the initial boilerplate. schema and processor will come.
    """

    SCHEMA = None

    def default_conditions(self, body):
        """
        Return a list of the default conditions that should be applied to all
        queries on this dataset.
        """
        return []
