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
    fasty can sustain 6+ million inserts/second on modern hardware.

    key differentiators from postgresql:
        1. async write pipeline - requests return before disk flush
        2. massive batch sizes - 10,000 ops per flush vs postgres ~100
        3. optional WAL - choose between speed and durability
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
    | Single INSERT          | 154,230/sec   | 5,000/sec    | 30.8x                            |
    | Batch INSERT (1000)    | 511,911/sec   | 30,000/sec   | 17.0x                            |
    | Parallel batch (50w)   | 4,616,352/sec | N/A          | --                               |
    | Indexed lookup         | 316,571/sec   | 15,000/sec   | 21.1x                            |
    | Range query            | 207,175/sec   | 50,000/sec   | 4.1x                             |
    | ORDER BY + LIMIT       | 155,669/sec   | 20,000/sec   | 7.8x (heap-based top-k)          |
    | JOIN (Inner)           | 160,382/sec   | 10,000/sec   | 16.0x                            |
    +===========================================================================================+

    benchmark environment: 28 CPU cores

    peak throughput achieved: 4,668,148 ops/sec (sustained load, 10 seconds)
    peak burst throughput:    4,265,558 ops/sec
    total benchmark:          49,284,200 ops in 13 seconds (3,876,352 ops/sec avg)


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
       fasty batches up to 20,000 operations before flushing to disk.
       this amortizes the cost of disk seeks across many operations.

       postgres default:  ~100 ops per WAL flush
       fasty:             20,000 ops per batch flush


    3. LSM TREE OPTIMIZATIONS (TIERED COMPACTION)
       -------------------------------------------------------------------------------------
       the underlying moss LSM tree is tuned for write throughput using a tiered-like strategy:
           - 1024+ pre-merger batches in memory (delays merging)
           - increased merge timeout (200ms) to allow larger batches
           - 32MB compaction buffer
           - async sync (NoSync: true)


    4. ZSTD COMPRESSION & BLOCK CACHE
       -------------------------------------------------------------------------------------
       all data is compressed using Zstd before storage, reducing disk usage and
       I/O bandwidth. an in-memory LRU block cache stores hot uncompressed values
       to eliminate decompression overhead for frequent reads.


    5. ASYNC PREFETCH ITERATORS
       -------------------------------------------------------------------------------------
       iterators run a background goroutine to prefetch and decompress the next
       items while the query engine processes the current ones. this overlaps
       CPU (decompression) and I/O latency.


    6. FINE-GRAINED WRITE THROTTLING
       -------------------------------------------------------------------------------------
       writes are rate-limited (~50MB/s) to prevent "IO storms" and unbounded
       memory growth during massive ingestion spikes, ensuring stable latency.


    7. HEAP-BASED TOP-K FOR ORDER BY + LIMIT
       -------------------------------------------------------------------------------------
       instead of sorting all rows then taking top N (O(n log n)),
       fasty maintains a heap of size K (O(n log k)).

       for "ORDER BY price DESC LIMIT 10" on 1M rows:
           traditional: sort 1M rows, take 10     -> O(n log n)
           fasty:       heap of size 10           -> O(n log 10) ~ O(n)


    8. PARALLEL TABLE SCANS
       -------------------------------------------------------------------------------------
       large table scans are parallelized across all CPU cores.
       rows are decoded and filtered concurrently.


    9. PRE-COMPILED WHERE CONDITIONS
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
     |                        STORAGE LAYER (custom moss LSM)                          |
     |                                                                                 |
     |   +--------------+  +--------------+  +--------------+  +--------------+       |
     |   | MemTable     |  | MemTable     |  | SSTable L0   |  | SSTable L1   |       |
     |   | (active)     |  | MemTable     |  | (recent)     |  | (compacted)  |       |
     |   | (frozen)     |  |              |  |              |  |              |       |
     |   +--------------+  +--------------+  +--------------+  +--------------+       |
     |                                                                                 |
     |   1024 pre-merger batches | 5000 ops per batch | tiered compaction             |
     |   Zstd compression | LRU block cache | tombstone delay | binary search merge   |
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


    JOIN (inner join)
    -------------------------------------------------------------------------------------
    select * from orders join users on orders.user_id = users.id limit 10


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


    CSV IMPORT
    -------------------------------------------------------------------------------------
    fasty includes a high-performance CSV import tool optimized for large datasets.

    go run import_csv.go [flags] <csv_file>

    Flags:
        -batch-size int    Rows per batch (default 10000)
        -workers int       Concurrent workers (default CPU cores * 2)

    Performance Features:
        - Concurrent batch processing with multiple workers
        - Large batch sizes (10k rows per batch)
        - Buffered I/O (64MB) for fast CSV reading
        - Optimized JSON generation
        - HTTP connection pooling (500 connections)
        - Real-time progress with import rates
        - Automatic column name sanitization
        - Error recovery and malformed row handling
        - All columns imported as strings for simplicity

    Example Performance:
        - 60k-200k rows/sec depending on configuration
        - Scales with CPU cores and batch size
        - Suitable for datasets with millions of rows


<========================================================================================>
api endpoints:

------------------------------------------------------------------------------------------
|                                    HTTP API                                            |
------------------------------------------------------------------------------------------

    POST /query
    -------------------------------------------------------------------------------------
    execute queries OR batch insert (unified endpoint)

    query request:  { "query": "find users where age > 21" }
    query response: { "success": true, "data": "[{...}, {...}]" }

    batch request:  { "table": "users", "rows": [{...}, {...}, ...] }
    batch response: { "success": true, "inserted": 1000 }

    durability depends on server mode:
        - default:   async writes, eventual persistence
        - --durable: WAL-backed, immediate durability (crash-safe)
        - --memory:  no persistence (fastest)


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

    # run (default mode - fast, eventual persistence)
    $ ./server
    * fasty database server started on :8000
      using 8 CPU cores with fasthttp

    # run (durable mode - WAL-backed, crash-safe)
    $ ./server --durable
    * fasty database server started on :8000
      using 8 CPU cores with fasthttp
      WAL enabled for immediate durability

    # run (in-memory mode - maximum speed, no persistence)
    $ ./server --memory
    * Starting in ULTRA-FAST in-memory mode (no persistence)
    * fasty database server started on :8000


    # create a table
    $ curl -X POST http://localhost:8000/query \
        -d '{"query": "create table users { id: int, name: string, age: int }"}'

    # insert data
    $ curl -X POST http://localhost:8000/query \
        -d '{"query": "insert users { id: 1, name: \"alice\", age: 25 }"}'

    # batch insert (1000 rows)
    $ curl -X POST http://localhost:8000/query \
        -d '{"table": "users", "rows": [{"id": 1, "name": "user1"}, ...]}'

    # query
    $ curl -X POST http://localhost:8000/query \
        -d '{"query": "find users where age > 21 order by age desc limit 10"}

    # import CSV data (server must be running on port 1337)
    $ go run import_csv.go data.csv
    $ go run import_csv.go -batch-size=50000 -workers=16 large_dataset.csv


------------------------------------------------------------------------------------------
|                                   benchmarking                                         |
------------------------------------------------------------------------------------------

    # run the benchmark suite
    $ ./server &
    $ go run benchmark_ultra.go

    example output (28 CPU cores):

    ============================================================
    Fasty Benchmark - 28 CPU cores
    ============================================================

    [1] Single INSERT (baseline)
    5000 ops in 85ms (58780 ops/sec)

    [2] Batch INSERT via /query
      batch size 100: 100000 ops in 552ms (181276 ops/sec)
      batch size 500: 100000 ops in 211ms (474684 ops/sec)
      batch size 1000: 100000 ops in 578ms (172996 ops/sec)

    [3] Parallel /query
      200k rows, 1000/batch, 10 workers: 200000 ops in 114ms (1749806 ops/sec)
      500k rows, 2000/batch, 20 workers: 480000 ops in 114ms (4193131 ops/sec)
      1000k rows, 5000/batch, 50 workers: 1000000 ops in 155ms (6451842 ops/sec)

    [6] Query performance
      index lookup: 2000 queries in 17ms (118897 q/sec)
      range query: 500 queries in 2ms (326393 q/sec)
      order by + limit: 200 queries in 1ms (192541 q/sec)

    [7] Sustained load (10 seconds)
    66411500 ops in 10.002s (6639669 ops/sec avg)

    [9] Latency distribution
      single inserts: p50=406us p95=1.026ms p99=1.77ms
      batch inserts: p50=1.871ms p95=3.751ms p99=4.493ms

    ============================================================
    Total operations: 68994200
    Total time: 13s
    Total throughput: 5219035 ops/sec
    ============================================================


<========================================================================================>
comparison with postgresql:

------------------------------------------------------------------------------------------
|                               feature comparison                                       |
------------------------------------------------------------------------------------------

    +-----------------------------+---------------------+---------------------+
    | Feature                     | fasty               | PostgreSQL          |
    +-----------------------------+---------------------+---------------------+
    | ACID Transactions           | [x] No              | [+] Yes             |
    | Durability Guarantee        | [+] Configurable    | [+] Immediate       |
    | Write Throughput            | [+] 6M+ ops/sec     | [~] 30k ops/sec     |
    | Complex JOINs               | [~] Basic           | [+] Yes             |
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
        - multi-region replication
        - production systems requiring battle-tested reliability


<========================================================================================>
internals:

------------------------------------------------------------------------------------------
|                              server modes                                              |
------------------------------------------------------------------------------------------

    fasty supports three operating modes:

    +-------------------+------------------+------------------+------------------+
    | Mode              | Throughput       | Durability       | Use Case         |
    +-------------------+------------------+------------------+------------------+
    | default           | 200k+ ops/sec    | Eventual         | Analytics, logs  |
    | --durable         | 50k+ ops/sec     | Immediate (WAL)  | Important data   |
    | --memory          | 500k+ ops/sec    | None             | Caching, dev     |
    +-------------------+------------------+------------------+------------------+


------------------------------------------------------------------------------------------
|                              async writer pipeline (default mode)                      |
------------------------------------------------------------------------------------------

    the async writer is the key to fasty's speed. here's how it works:

    1. request arrives at /query endpoint
    2. rows are encoded and added to in-memory buffer
    3. response returns immediately (sub-millisecond)
    4. background: when buffer reaches 20k ops OR 50ms passes:
       - buffer is swapped with empty buffer (lock-free)
       - old buffer sent to flush channel
       - one of 4 flush workers picks it up
       - worker calls LSM BatchSet()
    5. LSM batches another 20k ops before disk write

    total batching: 20k (async buffer) x 20k (LSM) = potential 400k ops per disk write

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
         +---> if len(buffer) >= 20000:
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
|                              WAL pipeline (--durable mode)                             |
------------------------------------------------------------------------------------------

    when --durable is enabled, writes go through a Write-Ahead Log first:

    1. request arrives at /query endpoint
    2. rows are encoded and written to WAL (sequential append)
    3. WAL is fsynced to disk (batch sync for efficiency)
    4. response returns (data is now crash-safe)
    5. background: data is asynchronously written to LSM tree

    WAL format:
    +--------+--------+--------+--------+--------+--------+
    | magic  | length |  crc32 | keylen |  key   | value  |
    | 4B     | 4B     |  4B    | 4B     |  var   | var    |
    +--------+--------+--------+--------+--------+--------+

    recovery:
        on startup, WAL is replayed to recover uncommitted data.
        after replay, WAL is cleared and LSM is flushed.

    code path:

    handleTurbo()
         |
         v
    DurableInserter.InsertMaps()
         |
         v
    DurableWriter.WriteBatch()
         |
         +---> WAL.WriteBatch() --> fsync (immediate durability)
         |
         +---> buffer append
         |
         +---> async flush to LSM (background)


------------------------------------------------------------------------------------------
|                              LSM tree configuration                                    |
------------------------------------------------------------------------------------------

    moss (custom fork: github.com/r-ef/moss) settings:

    CollectionOptions:
        MergerIdleRunTimeoutMS: 200     // batched merger (tiered-like)
        MaxPreMergerBatches:    1024    // huge memory buffer
        MinMergePercentage:     0.8     // tiered compaction

    StoreOptions:
        CompactionPercentage:   0.3
        CompactionBufferPages:  8192    // 32MB
        CompactionSync:         false

    PersistOptions:
        NoSync:                 true    // async fsync
        CompactionConcern:      Disable // no compaction during writes


    moss optimizations implemented:

    1. FASTER SEGMENT SORTING
       -------------------------------------------------------------------------------------
       replaced interface-based sort.Sort with slices.SortStableFunc using
       unsafe.Pointer casting for zero-copy comparisons. eliminates heap allocations
       and interface method calls during sorting.

    2. MODERN GO FEATURES
       -------------------------------------------------------------------------------------
       updated to Go 1.25.4, replaced reflect.SliceHeader with unsafe.Slice,
       optimized byte comparisons with bytes.Equal.

    3. TOMBSTONE DELAY OPTIMIZATION
       -------------------------------------------------------------------------------------
       when merging segments with high deletion ratios (>60%), delays the merge
       operation by 100ms to allow more writes that might cancel out deletions.
       reduces unnecessary I/O for workloads with many temporary deletions.

    4. BINARY SEARCH FOR LARGE BASE LAYERS
       -------------------------------------------------------------------------------------
       when the base layer is 5x larger than upper layers being merged (>10k entries),
       uses binary search on the base layer instead of heap merging. optimizes
       merging performance for write-heavy workloads with large accumulated data.

    5. REDUCED MERGING AGGRESSIVENESS
       -------------------------------------------------------------------------------------
       disabled forced merging with dirty base segments during persister busy periods,
       allowing CPU to focus on new incoming writes instead of re-merging data
       queued for persistence.


<========================================================================================>
source files:

    src/cmd/main.go                  - HTTP server, endpoint handlers
    src/cmd/engine/engine.go         - query parser, executor, catalog
    src/cmd/engine/batch.go          - batch insert implementations
    src/cmd/engine/async_writer.go   - async write pipeline (default mode)
    src/cmd/engine/durable_writer.go - WAL-backed write pipeline (--durable mode)
    src/cmd/engine/wal.go            - Write-Ahead Log implementation
    src/cmd/engine/fast_scan.go      - parallel scan, heap-based ORDER BY
    src/cmd/engine/mapping.go        - key-value encoding/decoding
    src/cmd/engine/cache.go          - query cache, result cache
    src/LSMTree/tree.go              - moss LSM wrapper (with Zstd + LRU cache)

    moss/                            - custom moss LSM fork (github.com/r-ef/moss)
        - optimized segment sorting (slices.SortStableFunc)
        - tombstone delay optimization
        - binary search for large base layers
        - modern Go 1.25.4 features

    benchmark_ultra.go               - performance benchmark
    import_csv.go                   - CSV import tool
    IMPORT_README.md                - CSV import documentation


<========================================================================================>

    "premature optimization is the root of all evil"
        - donald knuth

    "but sometimes you just need to go fast"
        - fasty


                                         *

```
