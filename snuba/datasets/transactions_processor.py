from snuba.processor import MessageProcessor


class TransactionsMessageProcessor(MessageProcessor):
    def process_message(self, message, metadata=None):
        # TODO: actually do something otherwise there will never be any
        # scalability issue to solve in this dataset.
        return None
