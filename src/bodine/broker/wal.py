"""
This is where the Write-Ahead log implementation will go

This WAL will be the database for the broker nodes.

Messages are stored as flat files on disk, one per-partition. Each partition is a directory of segment files (append-only binary files where messages are written to).

Writes always go to the end of the log, and reads are always forward from some offset. Messages are never updated or deleted by one ID, never queried by content. Having a full database when we only use a small subset of the typical feature-set is overkill for this application. 

Every 'produce' call appends a record to the file and optionally calls `fsync` to flush it to disk before acknowledging the producer.

Each record in the log is encoded the same way as the TCP messages, the file is just a sequence of those frames. Reading by offset requires scanning the whole file sequentially (slow) or maintaining a separate in-memory mapping offset numbers to byte positions in the file. Kafka keeps an index like that in `.index` files alongside `.log` segment files. For the first implementation we can just scan then worry about indexing later.


"""

