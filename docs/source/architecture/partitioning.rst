===========================================
Snuba Data Partitioning (under development)
===========================================

*This feature is under active development and is subject to change*

To support a higher volume of data, we are building out support for
datasets and storages that span multiple physical resources
(Kafka clusters, Redis instances, Postgres databases, ClickHouse clusters,
etc.) with the same schema. Across Sentry, data records will
have a logical partition assignment based on the data's organization_id. In Snuba,
we maintain a mapping of logical to physical partitions in
``settings.LOGICAL_PARTITION_MAPPING``.

In a future revision, this ``settings.LOGICAL_PARTITION_MAPPING`` will be
used along with ``settings.PARTITIONED_STORAGES`` to map queries and incoming
data from consumers to different ClickHouse clusters by overriding the
StorageSet key that exists in configuration.

===========================
Adding a physical partition
===========================

Add the logical:physical mapping
--------------------------------
To add a physical partition to the logical:physical mapping, or repartition, increment the
value of ``settings.LOCAL_PHYSICAL_PARTITIONS`` and change
the mapping of relevant partitions in ``settings.LOGICAL_PARTITION_MAPPING``.
Every logical partition **must** be assigned to a physical partition and the
valid values of physical partitions are in the range
of ``[0,settings.LOCAL_PHYSICAL_PARTITIONS)``.


TODO: adding storages, migrating subscriptions, etc.
----------------------------------------------------
