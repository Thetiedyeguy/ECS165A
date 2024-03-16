from lstore.table import Table, Record
from lstore.index import Index
from lstore.lock import LockManager
from lstore.query import Query

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.table = None
        self.queries = []
        self.rlock = set()
        self.wlock = set()
        self.insert_lock = set()
        pass

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, table, *args):
        self.queries.append((query, args))
        if self.table is None:
            self.table = table
        # use grades_table for aborting

        
    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        for query, args in self.queries:
            key = args[0]
            if key not in self.table.lock_manager_hash:
                self.insert_lock.add(key)
                self.table.lock_manager_hash[key] = LockManager()
            if key not in self.wlock.union(self.insert_lock):
                if self.table.lock_manager_hash[key].acquire_writer_lock():
                    self.wlock.add(key)
                else:
                    return self.abort()
        return self.commit()


    
    def abort(self):
        for key in self.rlock:
            self.table.lock_manager_hash[key].release_reader_lock()
        for key in self.wlock:
            self.table.lock_manager_hash[key].release_writer_lock()
        for key in self.insert_lock:
            del self.table.lock_manager_hash[key]
        return False

    

    def commit(self):
        for query, args in self.queries:
            query(*args)
            if query == Query.delete:
                key = args[0]
                del self.table.lock_manager_hash[key]
                if key in self.wlock:
                    self.wlock.remove(key)
                if key in self.insert_lock:
                    self.insert_lock.remove(key)
        
        for key in self.rlock:
            self.table.lock_manager_hash[key].release_reader_lock()
        for key in self.wlock:
            self.table.lock_manager_hash[key].release_writer_lock()
        for key in self.insert_lock:
            self.table.lock_manager_hash[key].release_writer_lock()
        return True

