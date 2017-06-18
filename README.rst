redpanda
========

`redpanda` is an exploration into an implementation of a distributed,
pandas-like dataframe using redis as the data store.

`redpanda` is highly experimental, work in progress, and most likely is not
optimized; feel free to look around, but be careful if you're planning to use
it for anything.

todo
----
* optimize DataFrame.dump() by using redis.mget per row or for the entire
  dataframe at once
