from lstore.table import Table, Record
from lstore.index import Index
import threading

class TransactionWorker:

    """
    # Creates a transaction worker object.
    """
    def __init__(self):
        self.stats = []
        self.transactions = []
        self.result = 0
        pass

    
    """
    Appends t to transactions
    """
    def add_transaction(self, t):
        self.transactions.append(t)

        
    """
    Runs all transaction as a thread
    """
    def run(self):
        self.thread = threading.Thread(target = self.__run, daemon = True)
        self.thread.start()
        # here you need to create a thread and call __run
    

    """
    Waits for the worker to finish
    """
    def join(self):
        self.thread.join()


    def __run(self):
        for transaction in self.transactions:
            self.stats.append(transaction.run())
        self.result = len(list(filter(lambda x: x, self.stats)))

