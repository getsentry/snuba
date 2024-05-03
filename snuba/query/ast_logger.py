from __future__ import annotations

import os
from typing import Any

from snuba.query.dsl_mapper import query_repr
from snuba.query.logical import Query


class Logger:
    def __init__(self) -> None:
        self.d: dict[str, list] = dict()

    def eat(self, tag: str, val: Any) -> None:
        if tag not in self.d:
            self.d[tag] = []
        self.d[tag].append(val)

    def __getitem__(self, key: Any) -> list:
        return self.d[key]


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
