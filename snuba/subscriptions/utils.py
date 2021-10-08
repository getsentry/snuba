from enum import Enum


class SchedulerMode(Enum):
    IMMEDIATE = "immediate"
    WAIT_FOR_SLOWEST_PARTITION = "wait-for-slowest"
