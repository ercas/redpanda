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

def decode_list(list_):
    """ Decodes a list of encoded strings, being sure not to decode None types

    Args:
        list_: A list of encoded strings.

    Returns:
        A list of decoded strings
    """

    return [
        item.decode() if item is not None
        else None
        for item in list_
    ]

def gen_key(column, row):
    """ Generates a cell name given a row and colum name

    Args:
        row: The name of the row.
        column: The name of the column.

    Returns:
        A string corresponding to the name of the cell (a.k.a. the key in the
        Redis database)
    """

    return ROW_COL_DIVIDER.join([str(column), str(row)])

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

    def set(self, column, row, value):
        """ Sets the value of a cell

        Args:
            column: The column of the dataframe.
            row: The row of the dataframe.
            value: The value to set the cell to.
        """

        self.redis.sadd("rows", row)
        self.redis.sadd("columns", column)
        self.redis.set(gen_key(column, row), value)

    def get(self, column, row):
        """ Gets the value of a cell

        Args:
            column: The column of the dataframe.
            row: The row of the dataframe.

        Returns:
            The value of the cell, or None if it does not exist.
        """

        result = self.redis.get(gen_key(column, row))
        if (result is None):
            return None
        else:
            return result.decode()

    def get_row(self, row):
        """ Returns a row as a dictionary

        Args:
            row: The row of the dataframe to get.

        Returns:
            The contents of the row, in the form of a dictionary.
        """

        columns = self.columns
        cell_names = tuple(gen_key(column, row) for column in columns)
        values = decode_list(self.redis.mget(*cell_names))
        return {columns[i]: values[i] for i in range(len(columns))}

    def get_column(self, column):
        """ Returns a column as a dictionary

        Args:
            column: The column of the dataframe to get.

        Returns:
            The contents of the column, in the form of a dictionary.
        """

        rows = self.rows
        cell_names = tuple(gen_key(column, row) for row in rows)
        values = decode_list(self.redis.mget(*cell_names))
        return {rows[i]: values[i] for i in range(len(rows))}

    def dump(self):
        """ Returns a nested dictionary where each dict[column][row] refers to
        the value of self.get(column, row) """

        return {
            column: self.get_column(column)
            for column in self.columns
        }

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
            row_output = [row]
            row_contents = self.get_row(row)
            for column in columns:
                cell = row_contents[column]
                if (cell is None):
                    row_output.append("na")
                else:
                    row_output.append(cell)
            output.append("\t".join(row_output))
        return "\n".join(output)

    ## CSV I/O #################################################################

    def to_csv(self, csv_path, write_index = True, per_row = True):
        """ Write the contents of the dataframe to a CSV file

        Args:
            csv_path: The path of the CSV file to write the dataframe contents
                to.
            write_index: A boolean describing whether or not to write the
                dataframe's index to the csv file.
            per_row: A boolean describing whether or not data should be
                retrieved on a row-by-row basis or all at once. Data is written
                out as chunks of it are received.
        """

        columns = self.columns
        with open(csv_path, "w") as f:
            # header
            if (write_index):
                f.write("%s\n" % ",".join([""] + columns))
            else:
                f.write("%s\n" % ",".join(columns))

            # content
            if (per_row):
                for row in self.rows:
                    row_content = self.get_row(row)
                    if (write_index):
                        f.write("%s," % row)
                    for column in columns[:-1]:
                        f.write("%s," % row_content[column])
                    f.write("%s\n" % row_content[columns[-1]])
            else:
                dump = self.dump()
                for row in self.rows:
                    if (write_index):
                        f.write("%s," % row)
                    for column in columns[:-1]:
                        f.write("%s," % dump[column][row])
                    f.write("%s\n" % dump[columns[-1]][row])

    def _from_csv(self, csv_path, read_index = True, per_row = True):
        """ Read the contents of a CSV file into the dataframe

        This function overwrites the contents of the dataframe and should not
        be used directly, because it can cause conflicts with existing data;
        use the module's from_csv function instead.

        Args:
            csv_path: The path of the CSV file to read the dataframe contents
                from.
            read_index: A boolean describing whether or not to read the first
                column of the CSV file as the dataframe's index.
            per_row: A boolean describing whether or not data should be
                imported on a row-by-row basis or all at once.
        """

        i = 0
        with open(csv_path, "r") as f:

            # dataframe chunk to be used if not importing on a per-row basis
            if (not per_row):
                data = {}

            for csv_row in csv.DictReader(f):
                if (read_index):
                    first = next(iter(csv_row))
                    row_name = csv_row.pop(first)
                else:
                    row_name = i
                    i += 1

                # data is chunked and imported every row
                if (per_row):
                    data = {}
                    for column_name in csv_row:
                        data[gen_key(column_name, row_name)] = csv_row[column_name]
                    self.redis.mset(data)

                # data is added to the dataframe chunk but not imported until
                # the very end
                else:
                    for column_name in csv_row:
                        data[gen_key(column_name, row_name)] = csv_row[column_name]

                self.redis.sadd("rows", row_name)

            # import the dataframe chunk if necessary
            if (not per_row):
                self.redis.mset(data)

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
