from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, cast

from flask import g

ExplainType = Literal["query_transform"]


@dataclass
class ExplainStep:
    category: str  # The class of step e.g. "processor"
    type: ExplainType  # A value that tells the frontend what data the step has
    name: str  # The specific name for the step e.g. "TimeSeriesProcessor"
    data: dict[str, Any] = field(
        default_factory=dict
    )  # Any extra information about the step


@dataclass
class ExplainMeta:
    original_ast: str = "TBD"
    steps: list[ExplainStep] = field(default_factory=list)

    def add_step(self, step: ExplainStep) -> None:
        self.steps.append(step)


def add_step(category: str, name: str, data: dict[str, Any] | None = None) -> None:
    try:
        if data is None:
            data = {}
        step = ExplainStep(category, "query_transform", name, data)

        if not hasattr(g, "explain_meta"):
            g.explain_meta = ExplainMeta()

        g.explain_meta.add_step(step)
    except RuntimeError:
        # Code is executing outside of a flask context
        return


def get_explain_meta() -> ExplainMeta | None:
    try:
        if hasattr(g, "explain_meta"):
            return cast(ExplainMeta, g.explain_meta)
        return None
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
