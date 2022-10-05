===========================================
Snuba Data Partitioning (under development)
===========================================

*This feature is under active development and is subject to change*

To support a higher volume of data, we are building out support for
datasets and storages that span multiple physical resources
(Kafka clusters, Redis instances, Postgres databases, ClickHouse clusters,
etc.) with the same schema. Across Sentry, data records will
have a logical partition assignment based on the data's organization_id. In Snuba,
we maintain a mapping of logical partitions to physical slices in
``settings.LOGICAL_PARTITION_MAPPING``.

In a future revision, this ``settings.LOGICAL_PARTITION_MAPPING`` will be
used along with ``settings.SLICED_STORAGES`` to map queries and incoming
data from consumers to different ClickHouse clusters by overriding the
StorageSet key that exists in configuration.

===========================
Adding a slice
===========================

Add the logical:physical mapping
--------------------------------
To add a physical partition to a storage set's logical:physical mapping, or repartition,
increment the slice count in ``settings.SLICED_STORAGE_SETS`` for the relevant
storage set. Change the mapping of the relevant storage set's
logical partitions in ``settings.LOGICAL_PARTITION_MAPPING``.
Every logical partition **must** be assigned to a slice and the
valid values of slices are in the range of ``[0,settings.SLICED_STORAGE_SETS[storage_set])``.

Defining sliced clusters
--------------------------------
To add a cluster with an associated (storage set key, slice) pair, add cluster definitions
to ``settings.SLICED_CLUSTERS`` in the desired environment's settings. Follow the same structure as
regular cluster definitions in ``settings.CLUSTERS``. In the ``storage_set_slices`` field, sliced storage
sets should be added in the form of ``(StorageSetKey, slice_id)`` where slice_id is in
the range ``[0,settings.settings.SLICED_STORAGE_SETS[storage_set])``.


TODO: adding storages, migrating subscriptions, etc.
----------------------------------------------------
