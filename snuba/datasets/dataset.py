from typing import Mapping, Optional, Sequence

from snuba.clickhouse.columns import ColumnSet
from snuba.datasets.entity import Entity
from snuba.datasets.entities.factory import get_entity_name
from snuba.datasets.plans.query_plan import ClickhouseQueryPlanBuilder
from snuba.query.validation import FunctionCallValidator


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

    # TODO: Still not sure if the Dataset class really needs the actual Entity object
    # or just the name.
    def __init__(
        self, *, entities: Sequence[Entity], default_entity: Optional[Entity] = None
    ) -> None:
        self.__entities: Mapping[str, Entity] = {
            get_entity_name(entity): entity for entity in entities
        }

        # TODO: This is a convenience while we slowly migrate everything to Entities. This way
        # every dataset can have a default entity which acts as a passthrough until we can
        # migrate the datasets to proper entities.
        self.__default_entity: Entity = None
        if default_entity:
            self.__default_entity = default_entity
        elif len(entities) == 1:
            self.__default_entity = entities[0]

        assert self.__default_entity is not None, "Datasets must have a default entity"

        # TODO: I want to maintain this API for now just to limit the scope of the code changes.
        # All the code that references Datasets needs to be evaluated as to whether it should take
        # entities instead, but for now keep these functions here.
        deprecated_functions = [
            "get_extensions",
            "get_query_processors",
            "get_all_storages",
            "get_writable_storage",
            "column_expr",
        ]
        for func in deprecated_functions:
            setattr(self, func, getattr(self.__default_entity, func))

    def get_all_entities(self) -> Sequence[Entity]:
        """
        Returns all entities for this dataset.
        This method should be used for schema bootstrap and migrations.
        It is not supposed to be used during query processing.
        """
        return self.__entities.items()

    def get_entity(self, entity_name: Optional[str]) -> Entity:
        if entity_name and entity_name in self.__entities:
            return self.__entities[entity_name]

        return self.__default_entity

    def get_query_plan_builder(
        self, entity_name: Optional[str] = ""
    ) -> ClickhouseQueryPlanBuilder:
        """
        Returns the component that transforms a Snuba query in a Storage query by selecting
        the storage and provides the directions on how to run the query.

        If the query is being executed on a single entity, that entity will be used to determine
        the query plan. In cases such as a join, the dataset will something something and
        then build the join query.
        """
        if entity_name:
            entity = self.get_entity(entity_name)
        else:
            entity = self.__default_entity

        return entity.get_query_plan_builder()

    def get_abstract_columnset(self) -> ColumnSet:
        """
        This is just a wrapper to maintain the legacy Dataset interface. It should be removed.
        """
        return self.__default_entity.get_data_model()

    # TODO: Entity or Dataset? Nothing seems to use this.
    def get_function_call_validators(self) -> Mapping[str, FunctionCallValidator]:
        """
        Provides a sequence of function expression validators for
        this dataset. The typical use case is the validation that
        calls to dataset specific functions are well formed.
        """
        return {}
