class DataSet(object):
    """
    A DataSet defines the complete set of data sources, schemas, and
    transformations that are required to:
        - Consume, transform, and insert data payloads from Kafka into Clickhouse.
        - Define how Snuba API queries are transformed into SQL.
    """
    def __init__(self, *args, **kwargs):
        self.SCHEMA = None
        self.PROCESSOR = None
        self.PROD = False

    def default_conditions(self, body):
        """
        Return a list of the default conditions that should be applied to all
        queries on this dataset.
        """
        return []

    def condition_expr(self, condition, body):
        """
        If this dataset has a particular way of turning an individual
        (column, operator, literal) condition tuple into SQL, return it here.
        """
        return None

    def column_expr(self, column_name, body):
        """
        If this dataset has a particular way of turning a column name
        into a SQL expression (eg. special column aliases that evaluate
        to something else), return it here.
        """
        return None


