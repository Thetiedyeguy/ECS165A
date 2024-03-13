import threading

class LockManager:

    def __init__(self):
        self.lock = threading.Lock()
        self.writer_active = False 
        self.reader_count = 0
    
    def release_reader_lock(self):
        with self.lock:
            if self.reader_count > 0:
                self.reader_count -= 1

    def release_writer_lock(self):
        with self.lock:
            self.writer_active = False

    def acquire_reader_lock(self):
        with self.lock:
            if self.writer_active:
                return False
            self.reader_count += 1
            return True

    def acquire_writer_lock(self):
        with self.lock:
            if self.writer_active or self.reader_count > 0:
                return False
            self.writer_active = True
            return True
