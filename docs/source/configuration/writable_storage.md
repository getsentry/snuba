# Writable Storage Schema

## Properties

- **version**: Version of schema.
- **kind**: Component kind.
- **name** *(string)*: Name of the writable storage.
- **storage** *(object)*:
  - **key** *(string)*: A unique key identifier for the storage.
  - **set_key** *(string)*: A unique key identifier for a collection of storages located in the same cluster.
- **schema** *(object)*:
  - **columns** *(array)*: Objects (or nested objects) representing columns containg a name, type and args.
  - **local_table_name** *(string)*: The local table name in a single-node ClickHouse.
  - **dist_table_name** *(string)*: The distributed table name in distributed ClickHouse.
- **query_processors** *(array)*: Names of QueryProcessor class which represents a transformation applied to the ClickHouse query.
- **stream_loader** *(object)*: The stream loader for a writing to ClickHouse. This provides what is needed to start a Kafka consumer and fill in the ClickHouse table.
  - **processor** *(string)*: Class name for Processor. Responsible for converting an incoming message body from the event stream into a row or statement to be inserted or executed against clickhouse.
  - **default_topic** *(string)*: Name of the Kafka topic to consume from.
  - **commit_log_topic** *(['string', 'null'])*: Name of the commit log Kafka topic.
  - **subscription_scheduled_topic** *(['string', 'null'])*: Name of the subscroption scheduled Kafka topic.
  - **subscription_scheduler_mode** *(['string', 'null'])*: The subscription scheduler mode used (e.g. partition or global). This must be specified if subscriptions are supported for this storage.
  - **subscription_result_topic** *(['string', 'null'])*: Name of the subscription result Kafka topic.
  - **replacement_topic** *(['string', 'null'])*: Name of the replacements Kafka topic.
  - **pre_filter** *(object)*: Name of class which filter messages incoming from stream.
    - **type** *(string)*: Name of StreamMessageFilter class key.
    - **args** *(array)*: Key/value mappings required to instantiate StreamMessageFilter class.
  - **dlq_policy** *(object)*: Name of class which filter messages incoming from stream.
    - **type** *(string)*: DLQ policy type.
    - **args** *(array)*: Key/value mappings required to instantiate DLQ class (e.g. topic name).
