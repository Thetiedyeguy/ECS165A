"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

from BTrees.OOBTree import OOBTree

class Index:
    def __init__(self, table):
        # One index for each table. All are empty initially.
        self.table = table
        self.indices = [None for i in range(table.num_columns)]
        self.column_num = dict()

    """
    # returns the location of all records with the given value on column "column"
    """

    def has_index(self, column):
        if self.indices[column] == None:
            print("false")
            return False
        else:
            print("true")
            return True

    def locate(self, column, value):
        column_tree = self.indices[column]
        if not column_tree.has_key(value):
            return []
        else:
            return column_tree[value]

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        return_list = []
        column_tree = self.indices[column]
        for listi in list(column_btree.values(min=begin, max=end)):
            return_list += listi
        return return_list




    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        if self.indices[column_number] == None:
            self.indices[column_number] = OOBTree()

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        self.indices[column_number] = None


    def push_index(self, columns, rid):
        for i in range(self.table.num_columns):
            if i != self.table.key:
                if self.indices[i] == None:
                    self.create_index(i)
                if not self.indices[i].has_key(columns[i]):
                    self.indices[i][columns[i]]= [rid]
                else:
                    self.indices[i][columns[i]].append(rid)
                self.column_num[columns[i]] = i
