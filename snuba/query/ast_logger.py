from __future__ import annotations

import os
from typing import Any, Iterator

from snuba.query.dsl_mapper import query_repr
from snuba.query.logical import Query


class Logger:
    """
    Used for tracking intermediate IO contracts of components.

    Say you have INPUT -> t1 -> t2 -> t3 -> OUT
    and you want to get the indermediate IO for each stage.
    You can do
    INPUT -> eat -> t1 -> eat -> t2 -> eat -> t3 -> eat -> OUT
    then the state of the logger will be
    [
    (IN, t1_out)
    (t2_in, t2_out)
    (t3_in, OUT)
    ]
    """

    def __init__(self) -> None:
        # the inner list is a "trace" (not sentry trace): -> t1 -> t2 -> t3 ->
        # the tuple is (in, out)
        self.logged: list[list[tuple[Any, Any]]] = []

    def begin(self, val: Any) -> None:
        """
        Begins a new "trace" (not sentry trace): -> t1 -> t2 -> t3 ->
        """
        self.logged.append([])
        self.isInput = True
        self.log(val)

    def log(self, val: Any) -> None:
        """
        Logs the given val as the current output/input.
        Swaps between treating val as input or output.

        eg. log(1) log(2) log(3) log(4)
            [(1,2),(3,4)]
        """
        if len(self.logged) == 0:
            raise ValueError("Begin must be called before eat")

        if self.isInput:
            self.logged[len(self.logged) - 1].append((val, None))
        else:
            # (val, None) -> (val, newval)
            t = self.logged[len(self.logged) - 1]
            t[len(t) - 1] = (t[len(t) - 1][0], val)

        self.isInput = not self.isInput

    def pipe(self, val: Any) -> None:
        """
        Logs the given val as the current output and the next input
        """
        self.log(val)
        self.log(val)

    def __getitem__(self, val: Any) -> list[tuple[Any, Any]]:
        return self.logged[val]

    def __iter__(self) -> Iterator[list[tuple[Any, Any]]]:
        return iter(self.logged)

    def __len__(self) -> int:
        return len(self.logged)


class ASTLogger:
    """
    This class is used to save runtime query object to python files.
    """

    def __init__(self, p: str):
        self.path = os.path.abspath(os.path.expanduser(p))
        # clear the file
        self.ctr = 0
        with open(self.path, "w"):
            pass
        self.write(
            """
from datetime import datetime

from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.dsl import (
    and_cond,
    column,
    divide,
    equals,
    greaterOrEquals,
    in_fn,
    less,
    literal,
    literals_tuple,
    plus,
    snuba_tags_raw,
    multiply
)
from snuba.query.expressions import CurriedFunctionCall, FunctionCall
from snuba.query.logical import Query
from snuba.query.mql.parser import parse_mql_query_body
import pytest

from_clause = QueryEntity(
    EntityKey.GENERIC_METRICS_DISTRIBUTIONS,
    get_entity(EntityKey.GENERIC_METRICS_DISTRIBUTIONS).get_data_model(),
)
"""
        )

    def get_unique_int(self) -> int:
        tmp = self.ctr
        self.ctr += 1
        return tmp

    def save(self, varname: str, val: Any, indent: int = 0) -> None:
        """
        Given val, saves its string representation as a python line.
        """
        self.write(f"{varname} = {repr(val)}", indent=indent)

    def save_query(self, varname: str, query: Query, indent: int = 0) -> None:
        self.write(f"{varname} = {query_repr(query)}", indent=indent)

    def write(self, txt: str = "", end: str = "\n", indent: int = 0) -> None:
        """
        write an arbitrary line to the python file.
        """
        s = []
        for i in range(indent):
            s.append("\t")
        s.append(txt)
        s.append(end)
        with open(self.path, "a") as f:
            f.write("".join(s))
