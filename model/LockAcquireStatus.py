"""
Authors:
Amanpreet Singh
Sharan Agrawal
"""
from enum import Enum


class LockAcquireStatus(Enum):
    """
    Type of output returns when one tries to acquire
    a lock through site manager
    """
    AllSitesDown = 0
    NoLock = 1
    GotLock = 2
    GotLockRecover = 3
