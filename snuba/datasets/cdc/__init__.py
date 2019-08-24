from snuba.datasets import Dataset


class CdcDataset(Dataset):
    def __init__(self, schema, *, processor, default_topic,
            default_replacement_topic, default_commit_log_topic, cdc_control_topic):
        super(CdcDataset, self).__init__(
            schema=schema,
            processor=processor,
            default_topic=default_topic,
            default_replacement_topic=default_replacement_topic,
            default_commit_log_topic=default_commit_log_topic,
        )
        self.__cdc_control_topic = cdc_control_topic

    def get_control_topic(self):
        return self.__cdc_control_topic
