from __future__ import annotations

import os
from abc import ABC, abstractmethod
from typing import Callable, Optional, Type, cast

from snuba.clickhouse.query import Query
from snuba.query.query_settings import QuerySettings
from snuba.utils.registered_class import RegisteredClass, import_submodules_in_directory
from snuba.web import QueryResult

SplitQueryRunner = Callable[[Query, QuerySettings], QueryResult]


class QuerySplitStrategy(ABC, metaclass=RegisteredClass):
    """
    Implements a query split algorithm. It works in a similar way as a
    QueryExecutionStrategy, it takes a query, request.query_settings and a query runner
    and decides if it should split the query into more efficient parts.
    If it can split the query, it uses the SplitQueryRunner to execute every chunk,
    otherwise it returns None immediately.

    The main difference between this class and the QueryPlanExecutionStrategy is that
    it relies on a smarter QueryRunner (SplitQueryRunner) than the one provided to the
    execution strategy. The runner this class receives is supposed to take care of
    running the DB query processors before executing the query on the database so that
    such responsibility is confined to the plan execution strategy.

    A QuerySplitStrategy must not fall back on the runner method to execute the
    entire query in case it cannot perform any useful split.
    Doing so would prevent following splitters defined by the storage to attempt the split.
    """

    @abstractmethod
    def execute(
        self,
        query: Query,
        query_settings: QuerySettings,
        runner: SplitQueryRunner,
    ) -> Optional[QueryResult]:
        """
        Executes and/or splits the query provided, like the equivalent method in
        QueryPlanExecutionStrategy.
        Since not every split algorithm can work on every query, this method should
        return None when the query is not supported by this strategy.
        """
        raise NotImplementedError

    @classmethod
    def get_from_name(cls, name: str) -> Type["QuerySplitStrategy"]:
        return cast(Type["QuerySplitStrategy"], cls.class_from_name(name))

    @classmethod
    def from_kwargs(cls, **kwargs: str) -> QuerySplitStrategy:
        return cls(**kwargs)

    @classmethod
    def config_key(cls) -> str:
        return cls.__name__


import_submodules_in_directory(
    os.path.dirname(os.path.realpath(__file__)), "snuba.datasets.plans.splitters"
)
