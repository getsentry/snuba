from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field

MAX_ROWS_TO_DELETE_DEFAULT = 100000


@dataclass
class DeletionSettings:
    is_enabled: int
    tables: Sequence[str]
    bulk_delete_only: bool = False
    allowed_columns: Sequence[str] = field(default_factory=list)
    max_rows_to_delete: int = MAX_ROWS_TO_DELETE_DEFAULT
    allowed_attributes_by_item_type: dict[str, list[str]] = field(default_factory=dict)
    partition_column: str | None = None
