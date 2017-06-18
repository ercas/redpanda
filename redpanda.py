#!/usr/bin/env python3

import csv
import redis

ROW_COL_DIVIDER = "%x%"

def from_csv(*args, **kwargs):
    """ Wrapper for DataFrame().from_csv()

    Args:
        See DataFrame.__from_csv.

    Returns:
        A populated DataFrame.
    """

    df = DataFrame()
    df._from_csv(*args, **kwargs)
    return df

def gen_key(row, column):
    """ Generates a cell name given a row and colum name

    Args:
        row: The name of the row.
        column: The name of the column.

    Returns:
        A string corresponding to the name of the cell (a.k.a. the key in the
        Redis database)
    """

    return ROW_COL_DIVIDER.join([str(row), str(column)])

class DataFrame():
    def __init__(self, db = 0, *redis_args, **redis_kwargs):
        self.db = db
        self.redis = redis.StrictRedis(db = db, *redis_args, **redis_kwargs)

    def scan(self, key):
        results = []
        cursor = 0
        while True:
            (cursor, scan_results) = self.redis.sscan(key, cursor)
            results += [x.decode() for x in scan_results]
            if (cursor == 0):
                return results

    @property
    def rows(self):
        """ Returns a sorted list of the dataframe's rows """

        return sorted(self.scan("rows"))

    @property
    def columns(self):
        """ Returns a sorted list of the dataframe's columns """

        return sorted(self.scan("columns"))

    def set(self, row, column, value):
        """ Sets the value of a cell

        Args:
            row: The row of the dataframe.
            column: The column of the dataframe.
            value: The value to set the cell to.
        """

        self.redis.sadd("rows", row)
        self.redis.sadd("columns", column)
        self.redis.set(gen_key(row, column), value)

    def get(self, row, column):
        """ Gets the value of a cell

        Args:
            row: The row of the dataframe.
            column: The column of the dataframe.

        Returns:
            The value of the cell, or None if it does not exist.
        """

        result = self.redis.get(gen_key(row, column))
        if (result is None):
            return None
        else:
            return result.decode()

    def dump(self):
        """ Returns a nested dictionary where each dict[row][column] refers to
        the value of self.get(row, column) """

        results = {}
        for row in self.rows:
            row_contents = {}
            for column in self.columns:
                row_contents[column] = self.get(row, column)
            results[row] = row_contents
        return results

    def __str__(self):
        """ Print the contents of the dataframe when it is used in the print
        function

        Returns:
            A string representation of the dataframe.
        """

        columns = self.columns
        output = []
        output.append("\t".join([""] + columns))
        for row in self.rows:
            row_contents = [row]
            for column in columns:
                cell = self.get(row, column)
                if (cell is None):
                    row_contents.append("na")
                else:
                    row_contents.append(cell)
            output.append("\t".join(row_contents))
        return "\n".join(output)

    ## CSV I/O #################################################################

    def to_csv(self, csv_path, write_index = True):
        """ Write the contents of the dataframe to a CSV file

        Args:
            csv_path: The path of the CSV file to write the dataframe contents
                to.
            write_index: A boolean describing whether or not to write the
                dataframe's index to the csv file.
        """

        dump = self.dump()
        columns = self.columns
        with open(csv_path, "w") as f:
            if (write_index):
                f.write("%s\n" % ",".join([""] + columns))
            else:
                f.write("%s\n" % ",".join(columns))
            for row in dump:
                if (write_index):
                    f.write("%s," % row)
                for column in columns[:-1]:
                    f.write("%s," % dump[row][column])
                f.write("%s\n" % dump[row][columns[-1]])

    def _from_csv(self, csv_path, read_index = True):
        """ Read the contents of a CSV file into the dataframe

        This function overwrites the contents of the dataframe and should not
        be used directly, because it can cause conflicts with existing data;
        use the module's from_csv function instead.

        Args:
            csv_path: The path of the CSV file to read the dataframe contents
                from.
            read_index: A boolean describing whether or not to read the first
                column of the CSV file as the dataframe's index.
        """

        i = 0
        with open(csv_path, "r") as f:
            for csv_row in csv.DictReader(f):
                if (read_index):
                    first = next(iter(csv_row))
                    row_name = csv_row.pop(first)
                else:
                    row_name = i
                    i += 1

                # rows are chunked to reduce the number of redis calls to 1
                args = {}
                for column_name in csv_row:
                    args[gen_key(row_name, column_name)] = csv_row[column_name]
                self.redis.mset(args)
                self.redis.sadd("rows", row_name)
            self.redis.sadd("columns", *tuple(csv_row.keys()))

if (__name__ == "__main__"):
    df = DataFrame()
    df.set("a", "a", 1)
    df.set("a", "b", 1)
    df.set("b", "b", 1)
    df.set("c", "d", 1)
    print(df.get("a", "b"))
    print(df.get("b", "a"))
    print("rows: %s" % df.rows)
    print("columns: %s" % df.columns)
    print(df)
    print(df.dump())
    print(df.to_csv("test.csv"))
    print(from_csv("/home/leaf/walk_durations.csv"))
