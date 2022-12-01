from TransactionStatus import TransactionStatus

class Transaction:
    def __init__(self, id, name, status, is_read_only) -> None:
        self.id = id
        self.name = name
        self.status = status
        self.is_read_only = is_read_only
        self.read_only_values = dict()
        self.variable_read = dict()
        self.variable_commit = dict()
        self.sites = list()
    
    def get_id(self):
        
        return self.id

    def get_name(self):

        return self.name

    def get_status(self):

        return self.status

    def set_status(self, status):
        
        self.status = status

    def get_variable_read(self):

        return self.variable_read

    def get_variable_commit(self):

        return self.variable_commit

    def clear_variable_commit(self):

        self.variable_commit = dict()