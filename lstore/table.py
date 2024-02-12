from lstore.index import Index
from time import time

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}
        self.index = Index(self)
        self.records = 0
        self.updates = 0
        self.pool = {}
        pass

    def __merge(self):
        print("merge is happening")
        pass

    def get_page_location(self, type):
        num_tail = self.updates
        num_base = self.records - num_tail
        page_range_idx = num_base % RECORD_PER_RANGE
        if type == "base":
            page_idx = num_base % RECORD_PER_PAGE
        else:
            page_idx = num_tail % RECORD_PER_PAGE
        return page_range_idx, page_idx

    def base_write(self, columns):
        page_range_idx, page_idx = self.get_page_location('base')
        for i, value in enumerate(columns):
            id = (i, 'base', self.name, page_range_idx, page_idx)
            page = get_page(id)

            page.write(value)
            offset = page.records - 1
            self.pool[id] = page

        rid = columns[0]
        address = [offset, 'base', self.name, page_range_idx, page_idx]
        self.page_directory[rid] = address
        self.num_records += 1

    def tail_write(self, columns):
        page_range_idx, page_idx = self.get_page_location('tail')
        for i, value in enumerate(columns):
            id = (i, 'tail', self.name, page_range_idx, page_idx)
            page = get_page(id)

            page.write(value)
            offset = page.records - 1
            self.pool[id] = page

        rid = columns[0]
        address = [offset, 'base', self.name, page_range_idx, page_idx]
        self.page_directory[rid] = address
        self.records += 1
        self.updates += 1

    def get_page(self, id){
        if id in self.pool:
            return self.pool[id]
        page = Page()
        self.pool[id] = page
        return page
    }
