from enum import Enum


class StateType(Enum):
    BOOTSTRAP = 0
    CONSUMING = 1
    SNAPSHOT_PAUSED = 2
    CATCHING_UP = 3
    FINISHED = 4
