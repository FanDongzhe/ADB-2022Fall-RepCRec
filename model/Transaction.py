from TransactionStatus import TransactionStatus

class Transaction:
    def __init__(self, id, name, status, is_read_only) -> None:
        self.id = id
        self.name = name
        self.status = TransactionStatus.Creating
        self.is_read_only = is_read_only
    
    def get_id(self):
        
        return self.id

    def get_name(self):

        return self.name

    def get_status(self):

        return self.status

    def set_status(self, status):
        
        self.status = status