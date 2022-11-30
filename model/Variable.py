from LockType import LockType
import copy

class Variable:

    def __init__(self, id, name, value, site_id) -> None:
        
        self.id = id
        self.name = name
        self.value = value
        self.site_id = site_id
        self.lock_type = None
        self.is_locked = None

    @classmethod
    def get_sites(self, id):
        if type(id) == str:
            id = int(id[1:])

        if id % 2 == 0:
            return 'all'
        else:
            return (id % 10) + 1

    def get_site_id(self):

        return self.site_id

    def get_value(self):

        return self.value

    def set_value(self, value):

        self.value = value

    def get_lock_type(self):

        return self.lock_type

    def set_lock_type(self, lock_type):

        self.lock_type = lock_type

    def copy_variable(self):

        return copy.deepcopy(self)