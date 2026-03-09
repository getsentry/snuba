from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence

from sentry_protos.snuba.v1.request_common_pb2 import TraceItemType

MAX_ROWS_TO_DELETE_DEFAULT = 100000


@dataclass
class DeletionSettings:
    is_enabled: int
    tables: Sequence[str]
    bulk_delete_only: bool = False
    allowed_columns: Sequence[str] = field(default_factory=list)
    max_rows_to_delete: int = MAX_ROWS_TO_DELETE_DEFAULT
    allowed_attributes_by_item_type: Dict[str, List[str]] = field(default_factory=dict)
    partition_column: Optional[str] = None


def get_trace_item_type_name(item_type: int) -> str:
    """
    Get the string name for a TraceItemType enum value.

    Uses the protobuf enum's Name() method and strips the "TRACE_ITEM_TYPE_" prefix,
    then converts to lowercase to match storage configuration naming conventions.

    Args:
        item_type: The integer value of the TraceItemType enum

    Returns:
        The string name used in storage configurations (e.g., "occurrence", "span")

    Raises:
        ValueError: If the item_type is not a valid TraceItemType enum value
    """
    try:
        # Get the full protobuf enum name (e.g., "TRACE_ITEM_TYPE_SPAN")
        # Cast to TraceItemType.ValueType to satisfy type checker
        full_name = TraceItemType.Name(item_type)  # type: ignore[arg-type]

        # Strip the "TRACE_ITEM_TYPE_" prefix and convert to lowercase
        prefix = "TRACE_ITEM_TYPE_"
        if not full_name.startswith(prefix):
            raise ValueError(f"Unexpected TraceItemType name format: {full_name}")

        return full_name[len(prefix) :].lower()
    except ValueError as e:
        # This happens when item_type is not a valid enum value
        raise ValueError(
            f"Unknown TraceItemType value: {item_type}. Must be a valid TraceItemType enum value."
        ) from e
