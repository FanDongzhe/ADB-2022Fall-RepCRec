from enum import Enum

class SiteStatus(Enum):
    Up = 0
    Down = 1
    Recover = 2
    Fail = 3