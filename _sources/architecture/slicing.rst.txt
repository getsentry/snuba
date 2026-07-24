===========================================
Snuba Data Slicing (under development)
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
used along with ``settings.SLICED_STORAGE_SETS`` to map queries and incoming
data from consumers to different ClickHouse clusters using a
(StorageSetKey, slice_id) pairing that exists in configuration.

===========================
Configuring a slice
===========================

Mapping logical partitions : physical slices
----------------------------------------------
To add a slice to a storage set's logical:physical mapping, or repartition,
increment the slice count in ``settings.SLICED_STORAGE_SETS`` for the relevant
storage set. Change the mapping of the relevant storage set's
logical partitions in ``settings.LOGICAL_PARTITION_MAPPING``.
Every logical partition **must** be assigned to a slice and the
valid values of slices are in the range of ``[0,settings.SLICED_STORAGE_SETS[storage_set])``.

Defining ClickHouse clusters in a sliced environment
----------------------------------------------------

Given a storage set, there can be three different cases:

1. The storage set is not sliced
2. The storage set is sliced and no mega-cluster is needed
3. The storage set is sliced and a mega-cluster is needed

A mega-cluster is needed when there may be partial data residing on different sliced
ClickHouse clusters. This could happen, for example, when a logical partition:slice
mapping changes. In this scenario, writes of new data will be routed to the new slice,
but reads of data will need to span multiple clusters. Now that queries need to work
across different slices, a mega-cluster query node will be needed.

For each of the cases above, different types of ClickHouse cluster
configuration will be needed.

For case 1, we simply define clusters as per usual in ``settings.CLUSTERS``.

For cases 2 and 3:

To add a sliced cluster with an associated (storage set key, slice) pair, add cluster definitions
to ``settings.SLICED_CLUSTERS`` in the desired environment's settings. Follow the same structure as
regular cluster definitions in ``settings.CLUSTERS``. In the ``storage_set_slices`` field, sliced storage
sets should be added in the form of ``(StorageSetKey, slice_id)`` where slice_id is in
the range ``[0,settings.SLICED_STORAGE_SETS[storage_set])`` for the relevant ``StorageSetKey``.


Preparing the storage for sharding
----------------------------------
A storage that should be sharded requires setting the partition key column that will be used
to calculate the logical partition and ultimately the slice ID for how to query the destination
data.

This is done with the `partition_key_column_name` property in the storage schema (we do not
support sharded storages for non-YAML based entities). You can see an example of how one
might shard by organization_id in generic_metrics_sets and generic_metrics_distributions
dataset YAML files.

Adding sliced Kafka topics
---------------------------------
In order to define a "sliced" Kafka topic, add ``(default logical topic name, slice id)`` to
``settings.SLICED_KAFKA_TOPIC_MAP``. This tuple should be mapped to a custom physical topic
name of the form ``logical_topic_name-slice_id``. Make sure to add the corresponding broker
configuration details to ``settings.SLICED_KAFKA_BROKER_CONFIG``. Here, use the same tuple
``(default logical topic name, slice id)`` as the key, and the broker config info as the value.

Example configurations:

``SLICED_KAFKA_TOPIC_MAP`` = {("snuba-generic-metrics", 1): "snuba-generic-metrics-1"}

``SLICED_KAFKA_BROKER_CONFIG`` = {("snuba-generic-metrics", 1): BROKER_CONFIG}

These types of topics can be "sliced": raw topics, replacements topics, commit log topics,
subscription scheduler topics. Note that the slicing boundary stops at this point and
the results topics for subscriptions cannot be sliced.


=================================
Working in a Sliced Environment
=================================

Starting a sliced consumer
-----------------------------

First, ensure that your slicing configuration is set up properly: ``SLICED_STORAGE_SETS``,
``SLICED_CLUSTERS``, ``SLICED_KAFKA_TOPIC_MAP``, and ``SLICED_KAFKA_BROKER_CONFIG``.
See above for details.

Start up ``snuba consumer`` as per usual, with an extra flag ``--slice-id`` set equal
to the slice number you are reading from.


TODO: handling subscriptions, scheduler and executor, etc.
----------------------------------------------------------
