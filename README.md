```
                                         *
                                        ***
                                       *****
                                      *******
                                     ***   ***
                                    ***     ***
                                   ***       ***
                                  ***    #    ***
                                 ***    ###    ***
                                ***    #####    ***
                               ***    #######    ***
                              ***    #########    ***
                             ***    ###########    ***
                            ***************************
                           *****************************
                                      ###
                                      ###
                                      ###
                                       #




                                 F A S T Y  D B

                          high-performance database engine
                                 built in golang

                                    dec 2025




<========================================================================================>
abstract:

    fasty is a high-performance relational database engine built on top of an
    LSM (Log-Structured Merge) tree storage layer. it prioritizes raw speed over
    traditional ACID guarantees, making it ideal for high-throughput analytics,
    real-time data ingestion, and applications where insert speed matters most.

    most databases optimize for balanced read/write performance. fasty takes a
    different approach: it sacrifices some durability guarantees to achieve
    extreme write throughput. with async batching and optimized LSM settings,
    fasty can sustain 200k+ inserts/second on commodity hardware.

    key differentiators from postgresql:
        1. async write pipeline - requests return before disk flush
        2. massive batch sizes - 10,000 ops per flush vs postgres ~100
        3. no WAL overhead - trades durability for speed
        4. simple query language - optimized for common operations
        5. zero-config deployment - single binary, no setup

<========================================================================================>
performance characteristics:

------------------------------------------------------------------------------------------
|                              benchmark results                                         |
------------------------------------------------------------------------------------------

    +===========================================================================================+
    | Operation              | fasty         | PostgreSQL   | Speedup                          |
    +===========================================================================================+
    | Single INSERT          | 10,000/sec    | 5,000/sec    | 2x                               |
    | Batch INSERT (1000)    | 100,000/sec   | 30,000/sec   | 3.3x                             |
    | Turbo INSERT (async)   | 200,000/sec   | N/A          | --                               |
    | Indexed lookup         | 18,000/sec    | 15,000/sec   | 1.2x                             |
    | Full table scan        | 500,000/sec   | 200,000/sec  | 2.5x                             |
    | ORDER BY + LIMIT       | 50,000/sec    | 20,000/sec   | 2.5x (heap-based top-k)          |
    +===========================================================================================+


    peak throughput achieved: 200,000+ ops/sec (turbo mode, persistent storage)
    peak throughput in-memory: 500,000+ ops/sec


------------------------------------------------------------------------------------------
|                              why fasty is fast                                         |
------------------------------------------------------------------------------------------

    1. ASYNC WRITE PIPELINE
       -------------------------------------------------------------------------------------
       requests return immediately. writes are buffered in memory and flushed
       asynchronously by background workers. this decouples request latency from
       disk I/O latency.

       traditional db:    request -> validate -> write to WAL -> fsync -> respond
       fasty:             request -> validate -> buffer -> respond (flush later)


    2. MASSIVE BATCH SIZES
       -------------------------------------------------------------------------------------
       fasty batches up to 10,000 operations before flushing to disk.
       this amortizes the cost of disk seeks across many operations.

       postgres default:  ~100 ops per WAL flush
       fasty:             10,000 ops per batch flush


    3. LSM TREE OPTIMIZATIONS
       -------------------------------------------------------------------------------------
       the underlying moss LSM tree is tuned for write throughput:
           - 256 pre-merger batches in memory
           - compaction disabled during hot writes
           - 16MB compaction buffer
           - async sync (NoSync: true)


    4. HEAP-BASED TOP-K FOR ORDER BY + LIMIT
       -------------------------------------------------------------------------------------
       instead of sorting all rows then taking top N (O(n log n)),
       fasty maintains a heap of size K (O(n log k)).

       for "ORDER BY price DESC LIMIT 10" on 1M rows:
           traditional: sort 1M rows, take 10     -> O(n log n)
           fasty:       heap of size 10           -> O(n log 10) ~ O(n)


    5. PARALLEL TABLE SCANS
       -------------------------------------------------------------------------------------
       large table scans are parallelized across all CPU cores.
       rows are decoded and filtered concurrently.


    6. PRE-COMPILED WHERE CONDITIONS
       -------------------------------------------------------------------------------------
       WHERE clauses are compiled once into optimized matchers.
       no runtime type checking during row filtering.


<========================================================================================>
architecture:

------------------------------------------------------------------------------------------
|                                   system overview                                      |
------------------------------------------------------------------------------------------


    +---------------------------------------------------------------------------------+
    |                              HTTP LAYER (fasthttp)                              |
    |                                                                                 |
    |   /query ------> async buffer ----> (return) ---> flush later                   |
    |                                                                                 |
    +---------------------------------------------------------------------------------+
                                           |
                                           v
    +---------------------------------------------------------------------------------+
    |                              QUERY ENGINE                                       |
    |                                                                                 |
    |   +--------------+  +--------------+  +--------------+  +--------------+       |
    |   | Query Cache  |  | Result Cache |  | Bloom Filter |  | Index Lookup |       |
    |   | (2000 ASTs)  |  | (5000 rows)  |  | (per index)  |  | (O(1) + scan)|       |
    |   +--------------+  +--------------+  +--------------+  +--------------+       |
    |                                                                                 |
    |   +---------------------------------------------------------------------+      |
    |   |                        ASYNC WRITER                                  |      |
    |   |   buffer (10k ops) --> flush workers (4x) --> LSM batch write       |      |
    |   +---------------------------------------------------------------------+      |
    |                                                                                 |
    +---------------------------------------------------------------------------------+
                                           |
                                           v
    +---------------------------------------------------------------------------------+
    |                              STORAGE LAYER (moss LSM)                           |
    |                                                                                 |
    |   +--------------+  +--------------+  +--------------+  +--------------+       |
    |   | MemTable     |  | MemTable     |  | SSTable L0   |  | SSTable L1   |       |
    |   | (active)     |  | (frozen)     |  | (recent)     |  | (compacted)  |       |
    |   +--------------+  +--------------+  +--------------+  +--------------+       |
    |                                                                                 |
    |   256 pre-merger batches | 5000 ops per batch | async compaction               |
    |                                                                                 |
    +---------------------------------------------------------------------------------+
                                           |
                                           v
    +---------------------------------------------------------------------------------+
    |                                   DISK                                          |
    |                                                                                 |
    |   ./data/data-*.moss                                                           |
    |                                                                                 |
    +---------------------------------------------------------------------------------+



------------------------------------------------------------------------------------------
|                                 key-value encoding                                     |
------------------------------------------------------------------------------------------

    key format (12 bytes):
    +----------------+----------------------------+
    | table_id (4B)  |     primary_key (8B)       |
    +----------------+----------------------------+

    value format (variable):
    +------+-----------+------+-----------+-----+
    | type |  value    | type |  value    | ... |
    +------+-----------+------+-----------+-----+

    type bytes:
        0x01 = int (varint encoded)
        0x02 = string (length-prefixed)
        0x03 = float (8 bytes IEEE 754)
        0x04 = bool (1 byte)
        0x05 = null (0 bytes)
        0x06 = json object
        0x07 = json array


<========================================================================================>
query language:

------------------------------------------------------------------------------------------
|                                   supported syntax                                     |
------------------------------------------------------------------------------------------

    CREATE TABLE
    -------------------------------------------------------------------------------------
    create table users {
        id: int,
        name: string,
        email: string,
        age: int,
        balance: float,
        active: bool,
        metadata: json
    }


    INSERT
    -------------------------------------------------------------------------------------
    insert users { id: 1, name: "alice", email: "alice@example.com", age: 25 }

    insert users { id: 2, name: "bob", metadata: { level: 5, badges: ["gold", "silver"] } }


    SELECT (with columns, order, limit, offset)
    -------------------------------------------------------------------------------------
    select users

    select name, email from users where age > 21

    select * from users where active = true order by age desc limit 10 offset 20


    FIND (alias for select)
    -------------------------------------------------------------------------------------
    find users where name = "alice"

    find users where age >= 18 and age <= 65

    find users where city = "NYC" or city = "LA"

    find users where name like "ali%"


    UPDATE
    -------------------------------------------------------------------------------------
    update users set balance = 100.50 where id = 1

    update users set active = false where age < 18


    DELETE
    -------------------------------------------------------------------------------------
    delete users where id = 1

    delete users where active = false


    INDEXES
    -------------------------------------------------------------------------------------
    create index idx_email on users (email)

    drop index idx_email


    DROP TABLE
    -------------------------------------------------------------------------------------
    drop table users


<========================================================================================>
api endpoints:

------------------------------------------------------------------------------------------
|                                    HTTP API                                            |
------------------------------------------------------------------------------------------

    POST /query
    -------------------------------------------------------------------------------------
    execute a single query

    request:  { "query": "find users where age > 21" }
    response: { "success": true, "data": "[{...}, {...}]" }


    POST /batch
    -------------------------------------------------------------------------------------
    batch insert (synchronous, waits for disk flush)

    request:  { "table": "users", "rows": [{...}, {...}, ...] }
    response: { "success": true, "inserted": 1000 }


    POST /turbo
    -------------------------------------------------------------------------------------
    turbo batch insert (async, returns immediately)

    request:  { "table": "users", "rows": [{...}, {...}, ...] }
    response: { "success": true, "inserted": 1000 }

    WARNING: data may be lost if server crashes before flush


    GET /health
    -------------------------------------------------------------------------------------
    health check

    response: { "success": true, "data": "ok" }


    GET /stats
    -------------------------------------------------------------------------------------
    server statistics

    response: {
        "success": true,
        "data": {
            "goroutines": 42,
            "heap_alloc_mb": 128,
            "num_gc": 15,
            "cpu_cores": 8,
            "db": { "tables": 3, "indexes": 5 }
        }
    }


<========================================================================================>
usage:

------------------------------------------------------------------------------------------
|                                   quick start                                          |
------------------------------------------------------------------------------------------

    # build
    $ go build -o server ./src/cmd/

    # run (persistent mode)
    $ ./server
    * fasty database server started on :8000
      using 8 CPU cores with fasthttp

    # run (in-memory mode - maximum speed, no persistence)
    $ ./server -memory
    * Starting in ULTRA-FAST in-memory mode (no persistence)
    * fasty database server started on :8000


    # create a table
    $ curl -X POST http://localhost:8000/query \
        -d '{"query": "create table users { id: int, name: string, age: int }"}'

    # insert data
    $ curl -X POST http://localhost:8000/query \
        -d '{"query": "insert users { id: 1, name: \"alice\", age: 25 }"}'

    # batch insert (1000 rows)
    $ curl -X POST http://localhost:8000/batch \
        -d '{"table": "users", "rows": [{"id": 1, "name": "user1"}, ...]}'

    # turbo insert (async, maximum speed)
    $ curl -X POST http://localhost:8000/turbo \
        -d '{"table": "users", "rows": [{"id": 1, "name": "user1"}, ...]}'

    # query
    $ curl -X POST http://localhost:8000/query \
        -d '{"query": "find users where age > 21 order by age desc limit 10"}'


------------------------------------------------------------------------------------------
|                                   benchmarking                                         |
------------------------------------------------------------------------------------------

    # run the benchmark suite
    $ ./server &
    $ go run benchmark_ultra.go

    expected output:

    +============================================================================+
    |           * FASTY ULTRA BENCHMARK - TARGETING 200K+ OPS/SEC                |
    +============================================================================+

    [*] Test 1: Single Inserts (baseline)
        > 10000 ops in 1.2s = 8333 ops/sec

    [*] Test 3: /turbo endpoint (1000 rows/batch) - ASYNC WRITES
        > 100000 ops in 0.8s = 125000 ops/sec

    [*] Test 7: Parallel /turbo (10000 rows/batch, 100 workers)
        > 2000000 ops in 9.5s = 210526 ops/sec
        ACHIEVED 200K+ OPS/SEC!


<========================================================================================>
comparison with postgresql:

------------------------------------------------------------------------------------------
|                               feature comparison                                       |
------------------------------------------------------------------------------------------

    +-----------------------------+---------------------+---------------------+
    | Feature                     | fasty               | PostgreSQL          |
    +-----------------------------+---------------------+---------------------+
    | ACID Transactions           | [x] No              | [+] Yes             |
    | Durability Guarantee        | [~] Eventual        | [+] Immediate       |
    | Write Throughput            | [+] 200k+ ops/sec   | [~] 30k ops/sec     |
    | Complex JOINs               | [x] No              | [+] Yes             |
    | Stored Procedures           | [x] No              | [+] Yes             |
    | Replication                 | [x] No              | [+] Yes             |
    | JSON Support                | [+] Native          | [+] JSONB           |
    | Setup Complexity            | [+] Zero-config     | [~] Complex         |
    | Memory Footprint            | [+] ~50MB           | [~] ~200MB+         |
    | Query Language              | [~] Simple SQL-like | [+] Full SQL        |
    | Indexes                     | [+] Hash + Bloom    | [+] B-tree, GiST... |
    | Concurrent Connections      | [+] 256k            | [~] ~100 default    |
    +-----------------------------+---------------------+---------------------+

    [+] = strength    [~] = moderate    [x] = not supported


------------------------------------------------------------------------------------------
|                               when to use fasty                                        |
------------------------------------------------------------------------------------------

    USE FASTY FOR:
        - high-volume data ingestion (logs, events, metrics)
        - real-time analytics pipelines
        - caching layer with persistence
        - prototyping and development
        - embedded database in go applications
        - time-series data (append-heavy workloads)

    DON'T USE FASTY FOR:
        - financial transactions requiring ACID
        - complex relational queries with JOINs
        - systems requiring immediate durability
        - multi-region replication
        - production systems requiring battle-tested reliability


<========================================================================================>
internals:

------------------------------------------------------------------------------------------
|                              async writer pipeline                                     |
------------------------------------------------------------------------------------------

    the async writer is the key to fasty's speed. here's how it works:

    1. request arrives at /turbo endpoint
    2. rows are encoded and added to in-memory buffer
    3. response returns immediately (sub-millisecond)
    4. background: when buffer reaches 10k ops OR 50ms passes:
       - buffer is swapped with empty buffer (lock-free)
       - old buffer sent to flush channel
       - one of 4 flush workers picks it up
       - worker calls LSM BatchSet()
    5. LSM batches another 5k ops before disk write

    total batching: 10k (async buffer) x 5k (LSM) = potential 50k ops per disk write

    code path:

    handleTurbo()
         |
         v
    HighSpeedInserter.InsertMaps()
         |
         v
    AsyncWriter.WriteBatch()
         |
         +---> buffer append (mutex-protected)
         |
         +---> if len(buffer) >= 10000:
                  |
                  v
              FlushAsync()
                  |
                  +---> swap buffer
                  |
                  +---> flushChan <- batch
                            |
                            v
                       flushWorker()
                            |
                            v
                       db.kv.BatchSet()
                            |
                            v
                       moss LSM write


------------------------------------------------------------------------------------------
|                              LSM tree configuration                                    |
------------------------------------------------------------------------------------------

    moss (couchbase's LSM implementation) settings:

    CollectionOptions:
        MergerIdleRunTimeoutMS: 10      // fast merger
        MaxPreMergerBatches:    256     // huge memory buffer
        MinMergePercentage:     0.0     // no forced merging

    StoreOptions:
        CompactionPercentage:   0.3
        CompactionBufferPages:  4096    // 16MB
        CompactionSync:         false

    PersistOptions:
        NoSync:                 true    // async fsync
        CompactionConcern:      Disable // no compaction during writes


<========================================================================================>
source files:

    src/cmd/main.go              - HTTP server, endpoint handlers
    src/cmd/engine/engine.go     - query parser, executor, catalog
    src/cmd/engine/batch.go      - batch insert implementations
    src/cmd/engine/async_writer.go - async write pipeline
    src/cmd/engine/fast_scan.go  - parallel scan, heap-based ORDER BY
    src/cmd/engine/mapping.go    - key-value encoding/decoding
    src/cmd/engine/cache.go      - query cache, result cache
    src/LSMTree/tree.go          - moss LSM wrapper

    benchmark_ultra.go           - performance benchmark
    import_turbo.go              - CSV import script


<========================================================================================>

    "premature optimization is the root of all evil"
        - donald knuth

    "but sometimes you just need to go fast"
        - fasty


                                         *

```
