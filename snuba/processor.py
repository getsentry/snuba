class MessageProcessor(object):
    """
    The Processor is responsible for converting an incoming message body from the
    event stream into a row or statement to be inserted or executed against clickhouse.
    """
    # action types
    INSERT = 0
    REPLACE = 1

    def process_message(self, message, metadata=None):
        raise NotImplementedError
