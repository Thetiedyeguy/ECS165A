from lstore.table import Table, Record
from lstore.index import Index
from datetime import datetime
from time import process_time
from lstore.config import *

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table = table
        pass


    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        if primary_key not in self.table.page_directory or primary_key is SPECIAL_NULL:
            return False
        base_address = self.table.page_directory[primary_key]
        self.table.update_value(RID_COLUMN, base_address, SPECIAL_NULL)
        return True


    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):

        #Check if key exists, if it doesnt then return false
        if self.insert_helper_record_exists(columns[self.table.key]):
            return False

        self.table.inserted_record.acquire()
        #Obtain Info for meta_data
        rid = self.insert_helper_generate_rid()
        indirection = SPECIAL_NULL
        time = datetime.now().strftime("%Y%m%d%H%M%S")
        schema_encoding = '0' * self.table.num_columns
        schema_encoding = int(schema_encoding)

        meta_data = [indirection, rid, int(time), schema_encoding, rid]
        #new_record = Record(rid, columns[self.table.key], columns)
        columns = list(columns)
        meta_data.extend(columns)

        self.table.base_write(meta_data)
        self.table.inserted_record.release()
        return True

    #Helper function: retrieves number of records current exist, +1 to obtain new rid
    def insert_helper_generate_rid(self):
        rid = self.table.records + 906659671
        return rid

    def insert_helper_record_exists(self, key):
        key_index = self.table.index
        locations = key_index.locate(self.table.key, key)
        return len(locations) > 0

    def select(self, search_key, search_key_index, projected_columns_index):
        rids = []
        records = []
        column = []
        if search_key_index == self.table.key:
            rids.append(self.table.key_to_rid[search_key])
        else:
            rids.extend(self.table.get_rid(search_key_index, search_key))


        for rid in rids:
            record = self.table.get_record(rid)
            # print("record:", record)
            # if(rid != record[RID_COLUMN]):
                # print(rid, " ", record)
            column = record[METADATA:METADATA + self.table.num_columns + 1]

            # print(record[INDIRECTION_COLUMN])

            if record[INDIRECTION_COLUMN] != SPECIAL_NULL:
                rid_tail = record[INDIRECTION_COLUMN]

                record_tail = self.table.get_record(rid_tail)
                # print("tail record:", record_tail)
                column = record_tail[METADATA:METADATA + self.table.num_columns + 1]
                column[self.table.key] = record[METADATA + self.table.key]

            for i in range(self.table.num_columns):
                if projected_columns_index[i] == None:
                    column[i] = None
            record = Record(rid, search_key, column)
            records.append(record)

        return records

        # we may assume that select will never be called on a key that doesn't exist

    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retreive.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        rids = []
        records = []
        column = []
        if search_key_index == self.table.key:
            rids.append(self.table.key_to_rid[search_key])
        else:
            rids.extend(self.table.get_rid(search_key_index, search_key))


        for rid in rids:
            record = self.table.get_record(rid)
            # print("record:", record)
            # if(rid != record[RID_COLUMN]):
                # print(rid, " ", record)
            column = record[METADATA:METADATA + self.table.num_columns + 1]

            # print(record[INDIRECTION_COLUMN])
            tail_record = record
            relative_version = (relative_version * -1) + 1
            if record[INDIRECTION_COLUMN] != SPECIAL_NULL:
                for i in range(relative_version):
                    rid_tail = tail_record[INDIRECTION_COLUMN]
                    tail_record = self.table.get_record(rid_tail)

                    column = tail_record[METADATA:METADATA + self.table.num_columns + 1]
                    column[self.table.key] = record[METADATA + self.table.key]
                    if rid_tail == rid:
                        break


            record = Record(rid, search_key, column)
            records.append(record)

        return records


    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        columnList = list(columns)
        if primary_key not in self.table.key_to_rid.keys():
            return False
            # print("aha")
        rid = self.table.key_to_rid[primary_key]
        if rid not in self.table.page_directory or rid is SPECIAL_NULL:
            return False

        self.table.updated_record.acquire()

        base_record = self.table.get_record(rid)
        base_rid = base_record[RID_COLUMN]
        base_indirection = base_record[INDIRECTION_COLUMN]

        # if base_record[METADATA + 1:METADATA + self.table.num_columns] == [0,0,0,0]:
            # print("here")


        #information for metadata
        time = datetime.now().strftime("%Y%m%d%H%M%S")
        new_tail_rid = self.table.updates
        new_tail_encoding = ''
        new_tail_indirection = None

        #set indirection to base record or last tail record
        if base_indirection == SPECIAL_NULL:
            new_tail_indirection = rid
            for i in range(len(columnList)):
                if columnList[i] == None:
                    columnList[i] = base_record[i + METADATA]
                    new_tail_encoding += '0'
                else:
                    new_tail_encoding += '1'
        else:
            last_tail = self.table.get_record(base_indirection)
            for i in range(len(columnList)):
                if columnList[i] == None:
                    columnList[i] = last_tail[i + METADATA]
                    if (last_tail[SCHEMA_ENCODING_COLUMN] // (10 ** i)) % 10:
                        new_tail_encoding += '1'
                    else:
                        new_tail_encoding += '0'
                else:
                    new_tail_encoding += '1'
            new_tail_indirection =  last_tail[RID_COLUMN]

        meta_data = [new_tail_indirection, new_tail_rid, int(time), new_tail_encoding, base_rid]
        columnList[self.table.key] = SPECIAL_NULL
        meta_data.extend(columnList)
        self.table.tail_write(meta_data)
        # print("columnList:", meta_data, "base:", base_record)

        #change base encoding if needed
        base_encoding = base_record[SCHEMA_ENCODING_COLUMN]
        new_base_encoding = base_encoding or new_tail_encoding

        base_address = self.table.page_directory[rid]
        self.table.update_value(INDIRECTION_COLUMN, base_address, new_tail_rid)
        self.table.update_value(SCHEMA_ENCODING_COLUMN, base_address, new_base_encoding)

        self.table.update_record.release()
        
        return True

    """
    :param start_range: int         # Start of the key range to aggregate
    :param end_range: int           # End of the key range to aggregate
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        total_sum = 0
        no_record = True
        column_index = aggregate_column_index + METADATA
        for key in range(start_range, end_range + 1):
            if key in self.table.key_to_rid.keys():
                record = self.select(key, self.table.key, [1 for i in range(self.table.num_columns)])[0]
                value = record.columns[aggregate_column_index]
                if value != None:
                    total_sum += value
                no_record = False
        if no_record:
            return False
        return total_sum


    """
    :param start_range: int         # Start of the key range to aggregate
    :param end_range: int           # End of the key range to aggregate
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        total_sum = 0
        no_record = True
        column_index = aggregate_column_index + METADATA
        for key in range(start_range, end_range + 1):
            if key in self.table.key_to_rid.keys():
                record = self.select_version(key, self.table.key, [1 for i in range(self.table.num_columns)], relative_version)[0]
                value = record.columns[aggregate_column_index]
                if value != None:
                    total_sum += value
                no_record = False
        if no_record:
            return False
        return total_sum


    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
