# Readable Storage Schema

## Properties

- **version**: Version of schema.
- **kind**: Component kind.
- **name** *(string)*: Name of the readable storage.
- **storage** *(object)*:
  - **key** *(string)*: A unique key identifier for the storage.
  - **set_key** *(string)*: A unique key identifier for a collection of storages located in the same cluster.
- **schema** *(object)*:
  - **columns** *(array)*: Objects (or nested objects) representing columns containg a name, type and args.
  - **local_table_name** *(string)*: The local table name in a single-node ClickHouse.
  - **dist_table_name** *(string)*: The distributed table name in distributed ClickHouse.
- **query_processors** *(array)*: Names of QueryProcess class which represents a transformation applied to the ClickHouse query.
