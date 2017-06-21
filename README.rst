redpanda
========

``redpanda`` is an exploration into an implementation of a pandas-like dataframe
using Redis as the data store, allowing for concurrent access from multiple
processes on multiple machines.

The state of a ``redpanda.DataFrame`` is stored entirely in Redis, so every
operation should be performed on the latest state of the dataframe at the time
that the operation takes place. This may often not be true in practice because
a number of operations are done in Python to translate accesses to a
``redpanda.DataFrame`` into Redis commands, so you should take care to make sure
that a write to a dataframe cell prevents concurrent reads/writes from other
processes. This functionality is currently not included because it goes beyond
the scope of what ``redpanda`` was created for.

``redpanda`` is highly experimental, work in progress, and most likely is not
optimized; feel free to look around, but be careful if you're planning to use
it for anything.

demo
----

::

    >>> import redpanda
    >>> df = redpanda.DataFrame()
    Creating new Redis database db0
    >>>
    >>> # setting, printing, and getting
    >>> df["a"]["a"] = 1
    >>> df["a"]["b"] = 2
    >>> df["b"]["c"] = 3
    >>> print(df)
        a	b
    a	1	na
    b	2	na
    c	na	3
    >>> print(df["a"]["c"])
    None
    >>> print(df.dump())
    {'a': {'a': '1', 'b': '2', 'c': None}, 'b': {'a': None, 'b': None, 'c': '3'}}
    >>>
    >>> # connecting to an existing redpanda dataframe
    >>> df2 = redpanda.DataFrame(db = 0)
    Connected to Redis database db0
    >>> print(df2)
        a	b
    a	1	na
    b	2	na
    c	na	3
    >>> df2["z"]["z"] = 5
    >>> print(df["z"]["z"])
    5

..

todo
----
* pandas-like setting of columns
* pandas-like initialization from a multidimensional array or nested dicts
