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
        self.key_to_rid = {}
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
            id = (i, 'base', page_range_idx, page_idx)
            page = get_page(id)

            page.write(value)
            offset = page.records - 1
            self.pool[id] = page

        rid = columns[RID_COLUMN]
        address = [offset, 'base', page_range_idx, page_idx]
        self.page_directory[rid] = address
        self.num_records += 1
        self.rid_to_key[columns[self.key + METADATA]] = rid

    def tail_write(self, columns):
        page_range_idx, page_idx = self.get_page_location('tail')
        for i, value in enumerate(columns):
            id = (i, 'tail', page_range_idx, page_idx)
            page = get_page(id)

            page.write(value)
            offset = page.records - 1
            self.pool[id] = page

        rid = columns[RID_COLUMN]
        address = [offset, 'tail', page_range_idx, page_idx]
        self.page_directory[rid] = address
        self.records += 1
        self.updates += 1

    def get_rid(self, column_index, target):
        rids = []
        for rid in self.page_directory:
            record = self.find_record(rid)

    def convert_key(self, key):
        return key_to_rid[key]

    def get_value(self, column, address):
        id = (column, address[1], address[2], address[3])
        page = get_page(id)
        value = page.data[i * 8:(i + 1) * 8]

    def update_value(self, column, address, value):
        id = (column, address[1], address[2], address[3])
        page = get_page(id)
        page.data[i * 8:(i + 1) * 8] = value
        self.pool[id] = page

    def get_record(self, rid):
        record = []
        address = self.page_directory[rid]
        for i in range(METADATA + self.num_columns):
            result = self.get_value(i, location)
            record.append(result)

    def get_page(self, id):
        if id in self.pool:
            return self.pool[id]
        page = Page()
        self.pool[id] = page
        return page
