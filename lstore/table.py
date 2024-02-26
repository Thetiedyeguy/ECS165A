from lstore.index import Index
from time import time
from lstore.page import Page, PageRange
from lstore.config import *
import time



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
        self.rndbool = False
        self.time = [0.0, 0.0]
        pass

    def __merge(self):
        pass

    def get_page_location(self, rid = -1):
        num_base = self.records
        if(rid == -1):
            page_range_idx = num_base // RECORD_PER_RANGE
            page_idx = num_base // RECORD_PER_PAGE
        else:
            page_range_idx = (rid - 906659671) // RECORD_PER_RANGE
            page_idx =  -1
        page_idx = page_idx % PAGE_PER_RANGE

        return page_range_idx, page_idx

    def base_write(self, columns):
        page_range_idx, page_idx = self.get_page_location()
        for i, value in enumerate(columns):
            page = self.get_page(page_range_idx, i, 'base')

            pageRange = self.pool[page_range_idx]
            page.write(value)
            pageRange.base_pages[int(page_idx)][i] = page
            self.pool[page_range_idx] = pageRange

        offset = page.num_records - 1
        rid = columns[RID_COLUMN]
        address = [offset, 'base', page_range_idx, page_idx]
        self.page_directory[rid] = address
        self.records += 1
        self.key_to_rid[columns[self.key + METADATA]] = rid

    def tail_write(self, columns):
        page_range_idx, page_idx = self.get_page_location(columns[0])
        for i, value in enumerate(columns):
            pageRange = self.pool[page_range_idx]
            page = self.get_page(page_range_idx, i, 'tail')

            page.write(value)
            pageRange.tail_pages[pageRange.current_tail_idx - 1][i] = page
            self.pool[page_range_idx] = pageRange

        offset = page.num_records - 1
        rid = columns[RID_COLUMN]
        address = [offset, 'tail', page_range_idx, page_idx]
        self.page_directory[rid] = address
        self.updates += 1

    def get_rid(self, column_index, target):
        rids = []
        for rid in self.page_directory:
            record = self.get_record(rid)
            if record[column_index + METADATA] == target:
                rids.append(rid)
        return rids

    def convert_key(self, key):
        return self.key_to_rid[key]

    def get_value(self, column, address):
        page = self.get_page(address[2], column, address[1], False, address[3])
        value = page.get_value(address[0])
        return value

    def update_value(self, column, address, value):
        page = self.get_page(address[2], column, address[1], False, address[3])
        page.write(value, address[0])

    def get_record(self, rid):
        record = []
        address = self.page_directory[rid]
        for i in range(METADATA + self.num_columns):
            result = self.get_value(i, address)
            record.append(result)
        return record

    def get_page(self, page_range_idx, column, type, current = True, pageNumber = 0):
        if page_range_idx in self.pool:
            pageRange = self.pool[page_range_idx]
        else:
            pageRange = PageRange(self.num_columns + METADATA)
            self.pool[page_range_idx] = pageRange
        if(current):
            if(type == 'base'):
                page = pageRange.get_current_base(column)
            else:
                page = pageRange.get_current_tail(column)
        else:
            if(type == 'base'):
                page = pageRange.base_pages[int(pageNumber)][column]
            else:
                page = pageRange.get_current_tail(column)
        return page
