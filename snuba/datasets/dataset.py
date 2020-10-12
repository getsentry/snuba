from typing import Mapping, Optional, Sequence

from snuba.datasets.entity import Entity
from snuba.datasets.storage import Storage, WritableTableStorage
from snuba.query.extensions import QueryExtension
from snuba.query.logical import Query
from snuba.datasets.entities.factory import get_entity, EntityKey


class Dataset(object):
    # TODO: s/dataset/entity/g
    """
    A dataset represents a data model we can run a Snuba Query on.
    A data model provides a logical schema (today it is a flat table,
    soon it will be a graph of Entities).
    The dataset (later the Entity) has access to multiple Storage objects, which
    represent the physical data model. Each one represents a table/view on the
    DB we can query.
    The class is a facade to access the components used to write on the
    data model and to query the entities.

    The dataset is made of several Storage objects (later we will introduce
    entities between Dataset and Storage). Each storage represent a table/view
    we can query.

    When processing a query, there are three main steps:
    - dataset query processing. A series of QueryProcessors are applied to the
      query before deciding which Storage to use. These processors are defined
      by the dataset
    - the Storage to run the query onto is selected and the query is transformed
      into a Clickhouse Query. This is done by a ClickhouseQueryPlanBuilder. This object
      produces a plan that includes the Query contextualized on the storage/s, the
      list of processors to apply and the strategy to run the query (in case of
      any strategy more complex than a single DB query like a split).
    - storage query processing. A second series of QueryProcessors are applied
      to the query. These are defined by the storage.

    The architecture of the Dataset is divided in two layers. The highest layer
    provides the logic we use to deal with the data model. (writers, query processors,
    query planners, etc.). The lowest layer incldues simple objects that define
    the query itself (Query, Schema, RelationalSource). The lop layer object access and
    manipulate the lower layer objects.
    """

    def __init__(self, *, default_entity: EntityKey) -> None:
        # TODO: This is a convenience while we slowly migrate everything to Entities. This way
        # every dataset can have a default entity which acts as a passthrough until we can
        # migrate the datasets to proper entities.
        self.__default_entity = default_entity

    def get_default_entity(self) -> Entity:
        return get_entity(self.__default_entity)

    # TODO: Remove once entity selection moves to Sentry
    def select_entity(self, query: Query) -> EntityKey:
        return self.__default_entity

    # TODO: The following functions are shims to the Entity. They need to be evaluated one by one
    # to see which ones should exist at which level.
    def get_extensions(self) -> Mapping[str, QueryExtension]:
        return self.get_default_entity().get_extensions()

    def get_all_storages(self) -> Sequence[Storage]:
        return self.get_default_entity().get_all_storages()

    def get_writable_storage(self) -> Optional[WritableTableStorage]:
        return self.get_default_entity().get_writable_storage()
