from enum import Enum

class InstructionType(Enum):
    Begin = 0
    BeginRO = 1
    Read = 2
    Write = 3
    ReadOnly = 4
    End = 5