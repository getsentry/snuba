from snuba.clickhouse import Array


class DataSet(object):
    """
    A DataSet defines the complete set of data sources, schemas, and
    transformations that are required to:
        - Consume, transform, and insert data payloads from Kafka into Clickhouse.
        - Define how Snuba API queries are transformed into SQL.

    This is the the initial boilerplate. schema and processor will come.
    """

    def __init__(self, schema, processor, default_topic,
            default_replacement_topic, default_commit_log_topic):
        self._schema = schema
        self.__processor = processor
        self.__default_topic = default_topic
        self.__default_replacement_topic = default_replacement_topic
        self.__default_commit_log_topic = default_commit_log_topic

    def get_schema(self):
        return self._schema

    def get_processor(self):
        return self.__processor

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

    def get_default_topic(self):
        return self.__default_topic

    def get_default_replacement_topic(self):
        return self.__default_replacement_topic

    def get_default_commit_log_topic(self):
        return self.__default_commit_log_topic

    def get_default_replication_factor(self):
        return 1

    def get_default_partitions(self):
        return 1

    def column_expr(self, column_name, body):
        """
        Return an expression for the column name. Handle special column aliases
        that evaluate to something else.

        """
        return column_name
