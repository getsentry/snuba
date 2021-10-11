from enum import Enum


class SchedulingWatermarkMode(Enum):
    PARTITION = "partition"
    GLOBAL = "global"
