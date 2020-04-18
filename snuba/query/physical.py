from snuba.query.logical import Query as LogicalQuery

# This defines the type for the physical query that operates on storages.
# Obviously this is supposed to become more complex than just a type alias.
Query = LogicalQuery
