#!/usr/bin/env python3

import collections
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

class InitializationConflict(Exception):
    pass

class DataFrame(object):
    """ Implements pandas-DataFrame-like functionality by utilizing a specially
    structured Redis database

    Attributes:
        db: The Redis database the the dataframe is stored on.
        redis: A redis.StrictRedis instance connected to the database.
    """

    def __init__(self, db = None, *redis_args, **redis_kwargs):
        """ Initializes the DataFrame class

        Args:
            db: An integer representing the number of the Redis redpanda
                database to connect to, or None if a new one should be created.
        """

        keyspace = redis.StrictRedis().info("keyspace")

        if (db is None):
            populated_dbs = [int(db_name[2:]) for db_name in keyspace]
            db = 0
            while (db in populated_dbs):
                db += 1
        else:
            assert type(db) is int, "db must be an integer"

        self.db = db
        self.redis = redis.StrictRedis(db = db, *redis_args, **redis_kwargs)
        db_name = "db%d" % db

        # database validation - if connecting to an existing database, it must
        # either be an empty database or contain the "redpanda_version" key
        if (self.redis.get("redpanda_version") is None):
            # the db existing in the keyspace is already enough to assume that
            # it has keys
            if (db_name in keyspace):
                raise InitializationConflict(
                    "Existing database %s is not compatible with redpanda"
                    % db_name
                )
            else:
                print("Creating new Redis database %s" % db_name)
        else:
            print("Connected to Redis database %s" % db_name)

        # mark this database as a valid redpanda database
        self.redis.set("redpanda_version", 1)

    def scan(self, key):
        """ Scan a Redis set

        Args:
            key: The key of the Redis set.

        Returns:
            A list containing the contents of the set.
        """

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

    def dump_row(self, row):
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

    def dump_column(self, column):
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
            column: self.dump_column(column)
            for column in self.columns
        }

    def __getitem__(self, key):
        """ Returns a DataFrameColumn object that facilitates getting/setting
        of cell values """
        return DataFrameColumn(self, key)

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
            row_contents = self.dump_row(row)
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
                    row_content = self.dump_row(row)
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

        self.redis.flushdb()

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

class DataFrameColumn(object):
    """ Facilitates pandas dataframe-like setting and getting of cell values

    This class serves as providing syntactic sugar for setting and getting
    DataFrame values. Values can be set/retrieved via DataFrame()[column][row],
    as in pandas.

    Args:
        dataframe: A DataFrame object that this class sets and gets values from.
        column: The column of the dataframe that this class is accessing.
    """

    def __init__(self, dataframe, column):
        """ Initializes DataFrameColumn class

        Args:
            dataframe: A DataFrame object that this class sets and gets values
                from.
            column: The column of the dataframe that this class is accessing.
        """

        self.dataframe = dataframe
        self.column = column

    def __getitem__(self, key):
        """ Set a value in the dataframe

        Args:
            key: The row of the cell whose value to get.

        Returns:
            The value of the desired cell.
        """

        return self.dataframe.get(self.column, key)

    def __setitem__(self, key, value):
        """ Set a value in the dataframe

        Args:
            key: The row of the cell whose value to set.
        """

        self.dataframe.set(self.column, key, value)

    def __str__(self):
        """ Print the contents of this row in the dataframe when it is used in
        the print function

        Returns:
            A string representation of this dataframe column.
        """

        column_dict = self.dataframe.dump_column(self.column)
        output_rows = ["\t%s" % self.column]

        for key in sorted(column_dict.keys()):
            value = column_dict[key]
            if (value is None):
                output_rows.append("%s\tna" % key)
            else:
                output_rows.append("%s\t%s" % (key, value))
        return "\n".join(output_rows)

if (__name__ == "__main__"):
    df = DataFrame()
    df["a"]["a"] = 1
    df["a"]["b"] = 1
    df["b"]["b"] = 1
    df["c"]["d"] = 1
    print(df["a"]["b"])
    print(df["b"]["a"])
    print("rows: %s" % df.rows)
    print("columns: %s" % df.columns)
    print(df)
    print(df.dump())
    print(df.to_csv("test.csv"))
    print(from_csv("/home/leaf/walk_durations.csv"))
