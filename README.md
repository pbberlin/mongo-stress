A go program putting stress on a mongodb sharded cluster.
Multiple instances can launch unlimited numbers of threads with read, insert and update requests to a mongodb sharded cluster.
The oplog of each cluster member's primary is tailed.
Id's can be hashed or not.
Secondary data structures might be written or not.
All configurable via http interface.
http interface needs to be migrated towards dygraph.