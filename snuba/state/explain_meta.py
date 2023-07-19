from __future__ import annotations

import difflib
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Generator, Literal, cast

from flask import g

ExplainType = Literal["query_transform"]


@dataclass
class StepData:
    pass


@dataclass
class TransformStepData(StepData):
    original: str
    transformed: str
    diff: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        diff = difflib.ndiff(self.original.split("\n"), self.transformed.split("\n"))
        self.diff = [str(df) for df in diff]


@dataclass
class ExplainStep:
    category: str  # The class of step e.g. "processor"
    type: ExplainType  # A value that tells the frontend what data the step has
    name: str  # The specific name for the step e.g. "TimeSeriesProcessor"
    data: StepData = field(
        default_factory=StepData
    )  # Any extra information about the step


@dataclass
class ExplainMeta:
    original_ast: str = "TBD"
    steps: list[ExplainStep] = field(default_factory=list)

    def add_step(self, step: ExplainStep) -> None:
        self.steps.append(step)


@contextmanager
def with_query_differ(
    category: str, name: str, query: Any
) -> Generator[None, None, None]:
    original = str(query)
    yield
    transformed = str(query)
    add_transform_step(category, name, original, transformed)


def add_transform_step(
    category: str, name: str, original: str, transformed: str
) -> None:
    diff = difflib.ndiff(original.split("\n"), transformed.split("\n"))
    diff_data = [str(df) for df in diff]
    step_data = TransformStepData(
        original=original,
        transformed=transformed,
        diff=diff_data,
    )
    add_step("query_transform", category, name, step_data)


def add_step(
    step_type: ExplainType, category: str, name: str, data: StepData | None = None
) -> None:
    try:
        if data is None:
            data = StepData()
        step = ExplainStep(category, step_type, name, data)
        if (meta := get_explain_meta()) is not None:
            meta.add_step(step)
    except RuntimeError:
        # Code is executing outside of a flask context
        return


def set_original_ast(original_ast: str) -> None:
    meta = get_explain_meta()
    if not meta:
        return  # outside request context

    g.explain_meta.original_ast = original_ast


def get_explain_meta() -> ExplainMeta | None:
    try:
        if hasattr(g, "explain_meta"):
            return cast(ExplainMeta, g.explain_meta)
        g.explain_meta = ExplainMeta()
        return g.explain_meta
    except RuntimeError:
        # Code is executing outside of a flask context
        return None


def explain_cleanup() -> None:
    try:
        if hasattr(g, "explain_meta"):
            g.pop("explain_meta")
    except RuntimeError:
        # Code is executing outside of a flask context
        pass
