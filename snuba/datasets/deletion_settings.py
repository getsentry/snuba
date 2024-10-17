from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

MAX_ROWS_TO_DELETE_DEFAULT = 1000


@dataclass
class DeletionSettings:
    is_enabled: int
    tables: Sequence[str]
    allowed_columns: Sequence[str]
    max_rows_to_delete: int = MAX_ROWS_TO_DELETE_DEFAULT
