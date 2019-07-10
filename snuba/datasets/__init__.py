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

    # These method should be removed once we will have dataset specific query processing in
    # the dataset class instead of util.py and when the dataset specific logic for processing
    # Kafka messages will be in the dataset as well.

    def get_metadata_columns(self):
        raise NotImplementedError

    def get_promoted_tag_columns(self):
        raise NotImplementedError

    def get_promoted_context_tag_columns(self):
        raise NotImplementedError

    def get_promoted_context_columns(self):
        raise NotImplementedError

    def get_required_columns(self):
        raise NotImplementedError

    def column_expr(self):
        raise NotImplementedError

    def get_promoted_columns(self):
        # The set of columns, and associated keys that have been promoted
        # to the top level table namespace.
        return {
            'tags': frozenset(col.flattened for col in (self.get_promoted_tag_columns() + self.get_promoted_context_tag_columns())),
            'contexts': frozenset(col.flattened for col in self.get_promoted_context_columns()),
        }

    def get_column_tag_map(self):
        # For every applicable promoted column,  a map of translations from the column
        # name  we save in the database to the tag we receive in the query.
        promoted_context_tag_columns = self.get_promoted_context_tag_columns()

        return {
            'tags': {col.flattened: col.flattened.replace('_', '.') for col in promoted_context_tag_columns},
            'contexts': {},
        }

    def get_tag_column_map(self):
        # And a reverse map from the tags the client expects to the database columns
        return {
            col: dict(map(reversed, trans.items())) for col, trans in self.get_column_tag_map().items()
        }

    def get_promoted_tags(self):
        # The canonical list of foo.bar strings that you can send as a `tags[foo.bar]` query
        # and they can/will use a promoted column.
        return {
            col: [self.get_column_tag_map()[col].get(x, x) for x in self.get_promoted_columns()[col]]
            for col in self.get_promoted_columns()
        }
