from __future__ import annotations

import json
from typing import Any

from pydantic import BaseModel, BeforeValidator
from typing_extensions import Annotated


def parse_json(value: Any) -> Any:
    if isinstance(value, str):
        return json.loads(value)
    return value


class DeleteRequest(BaseModel):
    query: Annotated[DeleteQuery, BeforeValidator(parse_json)]


class DeleteQuery(BaseModel):
    columns: dict[str, list[str | int]]
