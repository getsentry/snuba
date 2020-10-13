from typing import Sequence

from snuba.datasets.entity import Entity
from snuba.query.logical import Query
from snuba.datasets.entities.factory import get_entity, EntityKey


class Dataset(object):
    # TODO: s/dataset/entity/g
    """
    A dataset represents a data model we can run a Snuba Query on.
    A data model provides a logical schema of a graph of entities (today it is
    just a single entity, but it will soon be multiple entities).

    The class is a facade to access the components used to write on the
    data model and to query the entities.

    When processing a query, there are three main steps:
    - dataset query processing. A series of QueryProcessors are applied to the
      query before deciding which Storage to use. These processors are defined
      by the entity.
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

    # TODO: Remove once entity selection moves to Sentry
    def select_entity(self, query: Query) -> EntityKey:
        return self.__default_entity

    def get_default_entity(self) -> Entity:
        return get_entity(self.__default_entity)

    def get_all_entities(self) -> Sequence[Entity]:
        return [self.get_default_entity()]
