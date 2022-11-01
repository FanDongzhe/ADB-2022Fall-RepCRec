from TransactionStatus import TransactionStatus

class Transaction:
    def __init__(self, id, name, status) -> None:
        self.id = id
        self.name = name
        self.status = TransactionStatus.Creating
    
    def get_id(self):
        return self.id

    def get_status(self):
        return self.status