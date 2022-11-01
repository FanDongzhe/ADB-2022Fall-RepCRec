from enum import Enum

class TransactionStatus(Enum):
    Creating = 0
    Running = 1
    Waiting = 2
    Abort = 3
    Commit = 4