"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:
    class node:
        def __init__ (self, leaf = True):
            self.keys = []
            self.values = []
            self.children = []
            self.leaf = leaf

    def __init__(self, table, max_children = 2):
        # One index for each table. All our empty initially.
        self.indices = [self.node() for _ in range(table.num_columns)]
        self.max_children = max_children

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        locations = []
        return self.locate_helper(self.indices[column], value, locations)


    def locate_helper(self, node, key, locations):
        i = 0
        while i < len(node.keys) and key > node.keys[i]:
            i += 1

        if i < len(node.keys) and key == node.keys[i]:
            if node.leaf:
                # If the node is a leaf node, append the locations of records
                locations.append(node.values[i])
            else:
                # If the node is not a leaf node, recursively search in the child node
                self.locate_helper(node.children[i], key, locations)

        elif not node.leaf:
            # If the key is not found and the node is not a leaf node, recursively search in the appropriate child node
            self.locate_helper(node.children[i], key, locations)

        return locations

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        results = []
        return self.locate_range_helper(self.indices[column], begin, end, results)


    def locate_range_helper(self, node, begin, end, results):
        i = 0
        while i < len(node.keys) and node.keys[i] < begin:
            i += 1
        while i < len(node.keys) and node.keys[i] <= end:
            if node.leaf:
                for rid in node.children[i].rids:
                    results.append(rid)
            else:
                self.locate_range_helper(node.children[i], begin, end, results)
            i += 1


    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        if column_number >= len(self.indices):
            print("Column index out of range")
            return
        if not self.indices[column_number]:
            self.indices[column_number] = self.Node()

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        if column_number >= len(self.indices):
            print("Column index out of range")
            return
        self.indices[column_number] = None


    def push_index(self, columns, rid):
        for i in range(1, self.table.num_columns):
            if self.indices[i] is None:
                self.create_index(i)
            if columns[i] not in self.indices[i]:
                self.indices[i][columns[i]]= [rid]
            else:
                self.indices[i][columns[i]].append(rid)
            self.column_num[columns[i]] = i
