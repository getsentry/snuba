from typing import Optional, Sequence

from snuba.datasets.entity import Entity
from snuba.datasets.plans.query_plan import ClickhouseQueryPlanBuilder
from snuba.query.processors import QueryProcessor


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

    def __init__(self, *, default_entity: Entity) -> None:
        # TODO: Right now there are no Datasets with more than one Entity. Once that changes,
        # the entity accessor methods will need to be rewritten.
        self.__default_entity = default_entity

    def get_entity(self, entity_name: str) -> Entity:
        return self.__default_entity

    def get_default_entity(self) -> Entity:
        return self.__default_entity

    def get_all_entities(self) -> Sequence[Entity]:
        return [self.__default_entity]

    def get_query_plan_builder(
        self, entity_name: Optional[str] = ""
    ) -> ClickhouseQueryPlanBuilder:
        """
        Returns the component that transforms a Snuba query in a Storage query by selecting
        the storage(s) and provides the directions on how to run the query.
        """
        entity = self.get_default_entity()

        return entity.get_query_plan_builder()

    def get_query_processors(self) -> Sequence[QueryProcessor]:
        """
        Return the query processors for this dataset. By default that will
        be the processors for the entity associated with the dataset.
        """
        return self.__default_entity.get_query_processors()
