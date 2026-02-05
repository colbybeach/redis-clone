# Next Steps / Ideas

Key Expiration (TTL): Implement the EXPIRE command. You’ll need a background goroutine (a "reaper") that periodically scans for and deletes expired keys.

Multiple Data Types: Move beyond strings. Support Lists (using Go slices or container/list) and Hashes (nested maps).

LRU Eviction: When your server hits a "max memory" limit, implement a Least Recently Used algorithm to delete the oldest keys.


# Things I built on my own: 

1. H commands (HSet, HGet, etc)
2. Delete commands
3. Expire commands (handlers, helpers, queues, etc)