# Entity Schema

## Properties

- **version**: Version of schema.
- **kind**: Component kind.
- **schema** *(array)*: Objects (or nested objects) representing columns containg a name, type and args.
- **name** *(string)*: Name of the entity.
- **readable_storage** *(string)*: Name of a ReadableStorage class which provides an abstraction to read from a table or a view in ClickHouse.
- **writable_storage** *(['string', 'null'])*: Name of a WritableStorage class which provides an abstraction to write to a table in ClickHouse.
- **query_processors** *(array)*: Represents a transformation applied to the ClickHouse query.
    - **processor** *(string)*: Name of QueryProcessor class responsible for the transformation applied to a query.
    - **args** *(object)*: Key/value mappings required to instantiate QueryProcessor class.
- **translation_mappers** *(object)*: Represents the set of rules used to translates different expression types.
  - **functions** *(array)*
      - **mapper** *(string)*: Name of Mapper class config key.
      - **args** *(object)*: Key/value mappings required to instantiate Mapper class.
  - **curried_functions** *(array)*
      - **mapper** *(string)*: Name of Mapper class config key.
      - **args** *(object)*: Key/value mappings required to instantiate Mapper class.
  - **subscriptables** *(array)*
      - **mapper** *(string)*: Name of Mapper class config key.
      - **args** *(object)*: Key/value mappings required to instantiate Mapper class.
- **validators** *(array)*: The validation logic used on the ClickHouse query.
    - **validator** *(string)*: Name of Validator class config key.
    - **args** *(object)*: Key/value mappings required to instantiate Validator class.
- **required_time_column** *(string)*: The name of the required time column specifed in schema.
