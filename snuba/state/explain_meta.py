from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, cast

from flask import g


@dataclass
class ExplainStep:
    category: str  # The type of step e.g. "processor"
    name: str  # The specific name for the step e.g. "TimeSeriesProcessor"
    data: dict[str, Any] = field(
        default_factory=dict
    )  # Any extra information about the step


@dataclass
class ExplainMeta:
    steps: list[ExplainStep] = field(default_factory=list)

    def add_step(self, step: ExplainStep) -> None:
        self.steps.append(step)


def add_step(category: str, name: str, data: dict[str, Any] | None = None) -> None:
    try:
        if data is None:
            data = {}
        step = ExplainStep(category, name, data)

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
