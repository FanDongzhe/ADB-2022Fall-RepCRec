from LockType import LockType
from Transaction import Transaction
from Variable import Variable

class Lock:
    def __init__(self, lock_type, transaction, variable) -> None:
        
        self.lock_type = lock_type
        self.transaction = transaction
        self.variable = variable

    def get_lock_type(self):

        return self.lock_type

    def set_lock_type(self, lock_type):

        self.lock_type = lock_type

    def get_transaction(self):

        return self.transaction

    def set_transaction(self, transaction):

        self.transaction = transaction

    def get_variable(self):

        return self.variable

    