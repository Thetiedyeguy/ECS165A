
class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)

    def has_capacity(self):
        return self.num_records < RECORD_PER_PAGE

    def write(self, value):
        if self.has_capacity():
            self.data[self.num_records * 8:(self.num_records + 1) * 8] = int(value).to_bytes(8, byteorder = 'big')
            self.num_records += 1
        else:
            raise Exception("Page is full")
        pass

class PageRange:
    def __init__(self):
        self.base_idx = 0
        self.tail_idx = 0
        self.base_page = [None]
        self.tail_page = []

    def make_base_page(self, idx):
        self.base_page[idx] = Page()

    def make_tail_page(self, idx):
        self.tail_page.append(Page())
        self.tail_idx++

    def inc_base_idx(self):
        self.base_idx++

    def current_base(self):
        return self.base_page[self.base_idx]

    def current_tail(self):
        return self.tail_page
