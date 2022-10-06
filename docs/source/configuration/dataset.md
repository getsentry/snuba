# Dataset Schema

## Properties

- **version**: Version of schema.
- **kind**: Component kind.
- **name** *(string)*: Name of the dataset.
- **is_experimental** *(boolean)*: Marks the dataset as experimental. Healthchecks failing on this dataset will not block deploys and affect Snuba server's SLOs.
- **entities** *(object)*:
  - **all** *(array)*: Names of entities associated with this dataset.
