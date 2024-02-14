from lstore.config import *

class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)

    def has_capacity(self):
        return self.num_records < RECORD_PER_PAGE

    def write(self, value, location = - 1):
        if self.has_capacity():
            if location == -1:
                location = self.num_records
            self.data[location * 8:(location + 1) * 8] = int(value).to_bytes(8, byteorder = 'big')
            self.num_records += 1
        else:
            raise Exception("Page is full")
        pass

    def get_value(self, location):
        return int.from_bytes(self.data[location * 8:(location + 1) * 8], "big")

class PageRange:
    def __init__(self, num_base_pages=16):
        self.base_pages = [None for _ in range(num_base_pages)]
        self.tail_pages = []  # Start with a single tail page
        self.current_base_idx = 0
        self.current_tail_idx = 0

    def make_tail_page(self):
        self.tail_pages.append(Page())
        self.current_tail_idx += 1

    def make_base_page(self, idx):
        self.base_pages[idx] = Page()
        self.current_base_idx += 1

    def get_current_base(self):
        if(self.current_base_idx != 0):
            if self.base_pages[self.current_base_idx - 1].has_capacity():
                return self.base_pages[self.current_base_idx - 1]
            else:
                self.make_base_page(self.current_base_idx)
                return self.base_pages[self.current_base_idx - 1]
        else:
            #raise Exception("No base pages here")
            self.make_base_page(self.current_base_idx)
            return self.base_pages[self.current_base_idx - 1]

    def get_current_tail(self):
        if(self.current_tail_idx != 0):
            if self.tail_pages[self.current_tail_idx - 1].has_capacity():
                return self.tail_pages[self.current_tail_idx - 1]
            else:
                self.make_tail_page()
                return self.tail_pages[self.current_tail_idx - 1]
        else:
            self.make_tail_page()
            return self.tail_pages[self.current_tail_idx - 1]
            #raise Exception("No tail pages here")
