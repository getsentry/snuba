from dataclasses import dataclass


@dataclass
class MergeInfo:
    result_part_name: str
    elapsed: float
    progress: float
    size: int

    @property
    # estimated time remaining in seconds
    def estimated_time(self) -> float:
        return self.elapsed / (self.progress + 0.0001)
