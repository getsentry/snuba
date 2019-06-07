from snuba.clickhouse import Array

class DataSet(object):
    """
    A DataSet defines the complete set of data sources, schemas, and
    transformations that are required to:
        - Consume, transform, and insert data payloads from Kafka into Clickhouse.
        - Define how Snuba API queries are transformed into SQL.

    This is the the initial boilerplate. schema and processor will come.
    """

    def __init__(self, schema):
        self._schema = schema

    def get_schema(self):
        return self._schema

    def default_conditions(self, body):
        """
        Return a list of the default conditions that should be applied to all
        queries on this dataset.
        """
        return []

    def row_from_processed_message(self, message):
        values = []
        columns = self.get_schema().get_columns()
        for col in columns:
            value = message.get(col.flattened, None)
            if value is None and isinstance(col.type, Array):
                value = []
            values.append(value)

        return values
