from Lock import Lock
from LockType import LockType

class LockTable:

    def __init__(self) -> None:
        
        self.locktable = dict()

    def get_locktable(self):

        return self.locktable

    def add_lock(self, locktype, transaction, variable):

        lock = Lock(lock_type= locktype, transaction= transaction, variable= variable)

        if variable not in self.locktable:
            self.locktable[variable] = list()

        for locks in LockTable:
            if lock == locks:
                return
        
        self.locktable[variable].append(lock)

    def get_lock_num(self, variable):

        if variable in self.locktable:
            return len(self.locktable[variable])

        return 0

    def delete_lock(self, variable, lock):

        #delete a lock from a variable
        if variable in self.locktable.keys():
            self.locktable[variable].remove(lock)
            if len(self.locktable[variable]) == 0:
                self.locktable.pop(variable)


    def delete_all_locks(self, variable):

        self.locktable.pop(variable)

    def is_locked(self, variable):

        #whether a variable is locked
        if variable in self.locktable:
            if len(self.locktable[variable]) == 0:
                return False
            else:
                return True
        
        return False

    def is_locked_transaction(self, transaction, variable, lock_type):
        
        #whether a variable is locked by a transaction for a specific lock type
        if variable in self.locktable:
            for lock in self.locktable:
                lock_transaction = lock.get_transaction()
                if lock_transaction.get_name() == transaction.get_name():
                    if lock_type == lock.get_lock_type():
                        return True
        
        return False


    def clear_lock(self, lock, variable):
        """
        Clear a particular lock from the variable

        Args:
            lock: Lock to be cleared
            variable: variable on which lock is to cleared
        """
        if variable in self.lock_map.keys():
            try:
                index = self.lock_map[variable].index(lock)
                self.lock_map[variable] = self.lock_map[variable][
                    :index] + self.lock_map[variable][index + 1:]
                if len(self.lock_map[variable]) == 0:
                    self.lock_map.pop(variable)
                return True
            except ValueError:
                pass
        return False

    def is_write_locked(self, variable):
        """
        Checks whether a write lock is set on variable

        Args:
            variable: Variable for which write lock is to be checked
        Returns:
            bool telling whether the write lock is set
        """
        if variable not in self.lock_map:
            return False
        else:
            for lock in self.lock_map[variable]:
                if lock.get_lock_type() == LockType.Write:
                    return True
            else:
                return False