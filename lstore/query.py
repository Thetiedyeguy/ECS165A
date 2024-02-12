from lstore.table import Table, Record
from lstore.index import Index


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
        pass
    
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):

        #Check if key exists, if it doesnt then return false
        if self.insert_helper_record_exists(columns, columns[self.table.key]):
            return False

        #Obtain Info for meta_data
        rid = self.insert_helper_generate_rid()
        indirection = None
        time = datetime.now().strftime("%Y%m%d%H%M%S")
        schema_encoding = '0' * self.table.num_columns 

        meta_data = [indirection, rid, int(time), schema_encoding]
        new_record = Record(rid, columns[self.table.key], columns)
        meta_data.extend(new_record.columns)

        self.table.base_write(meta_data)
        return True

    #Helper function: retrieves number of records current exist, +1 to obtain new rid
    def insert_helper_generate_rid(self):
        rid = self.table.records
        self.table.records += 1
        return rid
    
    def insert_helper_record_exists(self, columns, key):
        key_index = self.table.index.indices[0]
        locations = key_index.locate(columns, key)
        return len(locations) > 0
    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, search_key, search_key_index, projected_columns_index):
        output = []
        matchingRIDs = []
        returnColumns = []
        if self.index.indices[column] != None: matchingRIDs = self.index.locate(search_key_index, search_key)
        if len(matchingRIDs) == 0: return []
        for i in range(self.table.num_columns):
            if projected_columns_index[i] == 0:
                returnColumns[i] = None
            newOut = Record(rid, search_key, returnColumns)
            output.append(newOut)

    
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
        pass

    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        columnList = list(columns)
        rid = self.table.index.locate(self.table.key, primary_key)
        if rid not in self.table.page_directory or rid is None: 
            return False
        base_record = self.table.get_record(rid)
        base_indirection = base_record[INDIRECTION_COLUMN]


        #information for metadata
        time = datetime.now().strftime("%Y%m%d%H%M%S")
        new_tail_rid = self.table.num_records
        new_tail_encoding = ''
        new_tail_indirection = None

        if base_indirection == None:
            new_tail_indirection = rid
            for i in columnList:
                if columnList[i] == None:
                    new_tail_encoding += '0'
                else
                    new_tail_encoding += '1'
        else
            last_tail = self.table.get_record(base_indirection)
            tail_indirection =  last_tail[RID_COLUMN]
            encoding =  last_tail[SCHEMA_ENCODING_COLUMN]

        pass
    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        pass

    
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
        pass

    
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
