from lstore.index import Index
from time import time
from lstore.page import Page, PageRange
from lstore.config import *
import os
import struct


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
    def __init__(self, name, num_columns, key, path):
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
        self.path = path
        self.lru = LRU(self.path, self.name, self.num_columns)

    def __merge(self, page_range_idx):
        old_pageRange = self.pool[page_range_idx]
        old_pageRange.pinned += 1
        new_pageRange = PageRange(self.num_columns)
        self.pool[page_range_idx] = new_pageRange
        new_pageRange.idx = page_range_idx
        new_pageRange.pinned += 1
        begginning_idx = page_range_idx * RECORD_PER_RANGE
        for i in range(RECORD_PER_RANGE):
            for i in range(self.num_columns):
                print("placeholder")
        pass

    def open(self):
        metadata_path = self.path + '/' + self.name + '/' + 'metadata'
        if os.path.isfile(metadata_path):
            f = open(metadata_path, 'rb')
            fileContent = f.read()
            current_byte = 0
            struct.unpack("i" * ((len(fileContent) -24) // 4), fileContent[20:-4])
            page_range_amt = int.from_bytes(fileContent[current_byte:current_byte + RECORD_SIZE])
            current_byte += RECORD_SIZE
            for i in range(page_range_amt):
                page_range_idx = int.from_bytes(fileContent[current_byte:current_byte + RECORD_SIZE])
                current_byte += RECORD_SIZE
                path = self.path + '/' + self.name + '/' + str(page_range_idx)
                pageRange = self.lru.read_page(path)
                self.pool[page_range_idx] = pageRange
                self.lru.created(pageRange)
            directory_keys = int.from_bytes(fileContent[current_byte:current_byte + RECORD_SIZE])
            current_byte += RECORD_SIZE
            for i in range(directory_keys):
                key = int.from_bytes(fileContent[current_byte:current_byte + RECORD_SIZE])
                current_byte += RECORD_SIZE
                address = [None, None, None, None]
                address[0] = int.from_bytes(fileContent[current_byte:current_byte + RECORD_SIZE])
                current_byte += RECORD_SIZE
                length = int.from_bytes(fileContent[current_byte:current_byte + RECORD_SIZE])
                current_byte += RECORD_SIZE
                address[1] = fileContent[current_byte: current_byte + length].decode('utf-8')
                current_byte += length
                address[2] = int.from_bytes(fileContent[current_byte:current_byte + RECORD_SIZE])
                current_byte += RECORD_SIZE
                address[3] = int.from_bytes(fileContent[current_byte:current_byte + RECORD_SIZE])
                current_byte += RECORD_SIZE
                self.page_directory[key] = address
            keys = int.from_bytes(fileContent[current_byte:current_byte + RECORD_SIZE])
            current_byte += RECORD_SIZE
            for i in range(keys):
                key = int.from_bytes(fileContent[current_byte:current_byte + RECORD_SIZE])
                current_byte += RECORD_SIZE
                rid = int.from_bytes(fileContent[current_byte:current_byte + RECORD_SIZE])
                current_byte += RECORD_SIZE
                self.key_to_rid[key] = rid
            f.close()

    def close(self):
        metadata_path = self.path + '/' + self.name + '/' + 'metadata'
        folder = self.path + '/' + self.name
        if not os.path.exists(folder):
            os.mkdir(folder)
        current_byte = 0
        page_range_amt = len(self.pool)
        page_directory_size = (len(self.page_directory) * RECORD_SIZE * 5) + RECORD_SIZE
        key_to_rid_size = (len(self.key_to_rid) * RECORD_SIZE * 2) + RECORD_SIZE
        table_metadata_size = (3 * RECORD_SIZE * page_range_amt) + RECORD_SIZE
        data_size = page_directory_size + key_to_rid_size + table_metadata_size
        data = bytearray(data_size)
        data[current_byte: current_byte + RECORD_SIZE] = int(page_range_amt).to_bytes(8, byteorder = 'big')
        current_byte += RECORD_SIZE
        if page_range_amt != 0:
            pageRange = self.lru.oldest
            i = 0
            while pageRange != None:
                i+=1
                data[current_byte: current_byte + RECORD_SIZE] = int(pageRange.idx).to_bytes(8, byteorder = 'big')
                current_byte += RECORD_SIZE
                path = self.path + '/' + self.name + '/' + str(pageRange.idx)
                self.lru.delete()
                pageRange = self.lru.oldest
        data[current_byte: current_byte + RECORD_SIZE] = int(len(self.page_directory)).to_bytes(8, byteorder = 'big')
        current_byte += RECORD_SIZE
        for key in self.page_directory:
            data[current_byte: current_byte + RECORD_SIZE] = int(key).to_bytes(8, byteorder = 'big')
            current_byte += RECORD_SIZE
            address = self.page_directory[key]
            data[current_byte: current_byte + RECORD_SIZE] = int(address[0]).to_bytes(8, byteorder = 'big')
            current_byte += RECORD_SIZE
            b = address[1].encode('utf-8')
            data[current_byte:current_byte + RECORD_SIZE] = int(len(b)).to_bytes(8, byteorder = 'big')
            current_byte += RECORD_SIZE
            data[current_byte: current_byte + len(b)] = b[:]
            current_byte += len(b)
            data[current_byte: current_byte + RECORD_SIZE] = int(address[2]).to_bytes(8, byteorder = 'big')
            current_byte += RECORD_SIZE
            data[current_byte: current_byte + RECORD_SIZE] = int(address[3]).to_bytes(8, byteorder = 'big')
            current_byte += RECORD_SIZE
        data[current_byte: current_byte + RECORD_SIZE] = int(len(self.key_to_rid)).to_bytes(8, byteorder = 'big')
        current_byte += RECORD_SIZE
        for key in self.key_to_rid:
            data[current_byte: current_byte + RECORD_SIZE] = int(key).to_bytes(8, byteorder = 'big')
            current_byte += RECORD_SIZE
            rid = self.key_to_rid[key]
            data[current_byte: current_byte + RECORD_SIZE] = int(rid).to_bytes(8, byteorder = 'big')
            current_byte += RECORD_SIZE
        f = open(metadata_path, 'wb')
        f.write(data)
        f.close()

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
            pageRange.dirty = True
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
        page_range_idx, page_idx = self.get_page_location(columns[BASE_RID_COLUMN])
        for i, value in enumerate(columns):
            pageRange = self.pool[page_range_idx]
            pageRange.dirty = True
            page = self.get_page(page_range_idx, i, 'tail')

            page.write(value)
            page_idx = pageRange.current_tail_idx - 1
            pageRange.tail_pages[page_idx][i] = page
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
        path = self.path + '/' + self.name + '/' + str(page_range_idx)
        if page_range_idx in self.pool:
            pageRange = self.pool[page_range_idx]
            pageRange.idx = page_range_idx
            self.lru.accessed(pageRange)
        elif os.path.isfile(path):
            pageRange = self.lru.read_page(path)
            self.pool[page_range_idx] = pageRange
            self.lru.created(pageRange)
        else:
            pageRange = PageRange(self.num_columns + METADATA)
            pageRange.idx = page_range_idx
            self.pool[page_range_idx] = pageRange
            self.lru.created(pageRange)
        if(current):
            if(type == 'base'):
                page = pageRange.get_current_base(column)
            else:
                page = pageRange.get_current_tail(column)
        else:
            if(type == 'base'):
                page = pageRange.base_pages[int(pageNumber)][column]
            else:
                page = pageRange.tail_pages[int(pageNumber)][column]
        return page

    def page_range_in_pool(self, address):
        return address in self.pool.keys



class LRU():

    def __init__(self, path, name, columns):
        self.latest = None
        self.oldest = None
        self.path = path
        self.table_name = name
        self.num_pages = 0
        self.max_pages = 100
        self.num_columns = columns

    def created(self, pageRange):
        if self.oldest == None:
            self.oldest = pageRange
        if self.latest != None:
            pageRange.prev = self.latest
            self.latest.next = pageRange
        self.latest = pageRange
        self.num_pages += 16
        if self.isFull():
            self.delete()

    def delete(self):
        self.num_pages -= (self.oldest.current_tail_idx + 16)
        if self.oldest.dirty:
            path = self.path + '/' + self.table_name + '/' + str(self.oldest.idx)
            self.write_page(path, self.oldest)
        if self.oldest.next != None:
            self.oldest.next.prev = None
            temp = self.oldest.next
            del self.oldest
            self.oldest = temp
        else:
            del self.oldest
            self.oldest = None

    def accessed(self, pageRange):
        if self.latest != pageRange:
            if self.oldest != pageRange:
                pageRange.next.prev = pageRange.prev
                pageRange.prev.next = pageRange.next
                pageRange.prev = self.latest
                self.latest.next = pageRange
                self.latest = pageRange

    def isFull(self):
        return self.num_pages >= self.max_pages

    def read_page(self, path):
        f = open(path, 'rb')
        pageRange = PageRange(self.num_columns + METADATA)
        fileContent = f.read()
        struct.unpack("i" * ((len(fileContent) -24) // 4), fileContent[20:-4])
        current_byte = 0
        current_base_idx = int.from_bytes(fileContent[current_byte:current_byte + RECORD_SIZE])
        current_byte += RECORD_SIZE
        current_tail_idx = int.from_bytes(fileContent[current_byte:current_byte + RECORD_SIZE])
        current_byte += RECORD_SIZE
        for i in range(current_base_idx):
            pageRange.make_base_page(i)
            for j in range(self.num_columns + METADATA):
                page = pageRange.get_current_base(j)
                page.num_records = int.from_bytes(fileContent[current_byte:current_byte + RECORD_SIZE])
                current_byte += RECORD_SIZE
                page.tps = int.from_bytes(fileContent[current_byte:current_byte + RECORD_SIZE])
                current_byte += RECORD_SIZE
                page.data = bytearray(fileContent[current_byte:current_byte + PAGE_SIZE])
                current_byte += PAGE_SIZE
                page.dirty = False
                pageRange.base_pages[i][j] = page

        for i in range(current_tail_idx):
            pageRange.make_tail_page()
            for j in range(self.num_columns + METADATA):
                page = pageRange.get_current_tail(j)
                page.num_records = int.from_bytes(fileContent[current_byte:current_byte + RECORD_SIZE])
                current_byte += RECORD_SIZE
                page.tps = int.from_bytes(fileContent[current_byte:current_byte + RECORD_SIZE])
                current_byte += RECORD_SIZE
                page.data = bytearray(fileContent[current_byte:current_byte + PAGE_SIZE])
                current_byte += PAGE_SIZE
                pageRange.tail_pages[i][j] = page
        f.close()
        pageRange.dirty = False
        return pageRange

    def write_page(self, path, pageRange):
        current_byte = 0
        current_base_idx = pageRange.current_base_idx
        current_tail_idx = pageRange.current_tail_idx
        data = bytearray(((current_base_idx + current_tail_idx) * (PAGE_SIZE + PAGE_METADATA_BYTES) * self.num_columns) + PAGERANGE_METADATA_BYTES)
        data[current_byte: current_byte + RECORD_SIZE] = int(current_base_idx).to_bytes(8, byteorder = 'big')
        current_byte += RECORD_SIZE
        data[current_byte: current_byte + RECORD_SIZE] = int(current_tail_idx).to_bytes(8, byteorder = 'big')
        current_byte += RECORD_SIZE
        for i in range(current_base_idx):
            page = pageRange.base_pages[i]
            self.show_page(page)
            for j in range(self.num_columns + METADATA):
                data[current_byte: current_byte + RECORD_SIZE] = int(page[j].num_records).to_bytes(8, byteorder = 'big')
                current_byte += RECORD_SIZE
                data[current_byte: current_byte + RECORD_SIZE] = int(page[j].tps).to_bytes(8, byteorder = 'big')
                current_byte += RECORD_SIZE
                data[current_byte: current_byte + PAGE_SIZE] = page[j].data
                current_byte += PAGE_SIZE

        for i in range(current_tail_idx):
            page = pageRange.tail_pages[i]
            for j in range(self.num_columns + METADATA):
                data[current_byte: current_byte + RECORD_SIZE] = int(page[j].num_records).to_bytes(8, byteorder = 'big')
                current_byte += RECORD_SIZE
                data[current_byte: current_byte + RECORD_SIZE] = int(page[j].tps).to_bytes(8, byteorder = 'big')
                current_byte += RECORD_SIZE
                data[current_byte: current_byte + PAGE_SIZE] = page[j].data
                current_byte += PAGE_SIZE
        f = open(path, 'wb')
        f.write(data)
        f.close()


    def show_page(self, page):
        for i in range(512):
            string = ''
            for j in range(METADATA + self.num_columns):
                string += str(page[j].get_value(i)) + ' '
