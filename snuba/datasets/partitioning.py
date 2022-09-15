"""
The number of logical partitions used to distinguish between where records
should be stored. These do not require individual physical partitions but allow
for repartitioning with less code changes per physical change.
"""
SENTRY_LOGICAL_PARTITIONS = 256
