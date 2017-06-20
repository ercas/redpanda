redpanda
========

`redpanda` is an exploration into an implementation of a pandas-like dataframe
using Redis as the data store, allowing for concurrent access from multiple
processes on multiple machines.

the state of a `redpanda.DataFrame` is stored entirely in Redis, so every
operation should be performed on the latest state of the dataframe at the time
that the operation takes place. This may often not be true in practice because
a number of operations are done in Python to translate accesses to a
`redpanda.DataFrame` into Redis commands, so you should take care to make sure
that a write to a dataframe cell prevents concurrent reads/writes from other
processes. This functionality is currently not included because it goes beyond
the scope of what `redpanda` was created for.

`redpanda` is highly experimental, work in progress, and most likely is not
optimized; feel free to look around, but be careful if you're planning to use
it for anything.

todo
----
* pandas-like accessing and setting of cell values
