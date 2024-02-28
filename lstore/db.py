from lstore.table import Table
from lstore.config import *
import os
import struct

class Database():

    def __init__(self):
        self.tables = []
        self.path = ''
        pass

    # Not required for milestone1
    def open(self, path):
        self.path = path
        metadata_path = self.path + '/' + 'db'
        if os.path.isfile(metadata_path):
            f = open(metadata_path, 'rb')
            fileContent = f.read()
            current_byte = 0
            struct.unpack("i" * ((len(fileContent) -24) // 4), fileContent[20:-4])
            table_amt = int.from_bytes(fileContent[current_byte:current_byte + RECORD_SIZE])
            current_byte += RECORD_SIZE
            for i in range(table_amt):
                length = int.from_bytes(fileContent[current_byte:current_byte + RECORD_SIZE])
                current_byte += RECORD_SIZE
                name = fileContent[current_byte: current_byte + length].decode('utf-8')
                current_byte += length
                num_columns = int.from_bytes(fileContent[current_byte:current_byte + RECORD_SIZE])
                current_byte += RECORD_SIZE
                key = int.from_bytes(fileContent[current_byte:current_byte + RECORD_SIZE])
                current_byte += RECORD_SIZE
                table = self.create_table(name, num_columns, key)
                table.open()
            f.close()

        pass

    def close(self):
        if not os.path.exists(self.path):
            os.mkdir(self.path)
        metadata_path = self.path + '/' + 'db'
        current_byte = 0
        table_amt = len(self.tables)
        data = bytearray(RECORD_SIZE + (((2 * RECORD_SIZE) + MAX_STRING_LENGTH) * table_amt))
        data[current_byte: current_byte + RECORD_SIZE] = int(table_amt).to_bytes(8, byteorder = 'big')
        current_byte += RECORD_SIZE
        for i in range(table_amt):
            table = self.tables[i]
            b = table.name.encode('utf-8')
            data[current_byte:current_byte + RECORD_SIZE] = int(len(b)).to_bytes(8, byteorder = 'big')
            current_byte += RECORD_SIZE
            data[current_byte: current_byte + len(b)] = b[:]
            current_byte += len(b)
            data[current_byte: current_byte + RECORD_SIZE] = int(table.num_columns).to_bytes(8, byteorder = 'big')
            current_byte += RECORD_SIZE
            data[current_byte: current_byte + RECORD_SIZE] = int(table.key).to_bytes(8, byteorder = 'big')
            current_byte += RECORD_SIZE
            table.close()
        f = open(metadata_path, 'wb')
        f.write(data)
        f.close()
        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        table = Table(name, num_columns, key_index, self.path)
        self.tables.append(table)
        return table

    def show_tables(self):
        print("Table_Name\t|Columns\t|Key")
        for table in self.tables:
            print(f"{table.name}\t\t|{table.num_columns}\t\t|{table.key}")

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        for i in range(len(self.tables)):
            if(self.tables[i].name == name):
                new_tables = tables[:i]
                new_tables.extend(tables[i+1:])
                del tables[i]
                tables = new_tables
        pass


    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        for i in range(len(self.tables)):
            if(self.tables[i].name == name):
                return self.tables[i]
        return None
