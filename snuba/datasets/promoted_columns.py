from dataclasses import dataclass
from typing import Mapping, Iterable


@dataclass(frozen=True)
class PromotedColumnSpec:
    tag_column_mapping: Mapping[str, str]

    def get_tags(self) -> Iterable[str]:
        return self.tag_column_mapping.keys()

    def get_columns(self) -> Iterable[str]:
        return self.tag_column_mapping.values()

    def get_column_tag_map(self) -> Mapping[str, str]:
        return {col: tag for (tag, col) in self.tag_column_mapping.items()}
