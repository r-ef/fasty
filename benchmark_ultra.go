//go:build ignore

package main

import (
	"fmt"
	"math/rand"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	json "github.com/goccy/go-json"
	"github.com/valyala/fasthttp"
)

const baseURL = "http://localhost:8000"

var client = &fasthttp.Client{
	MaxConnsPerHost:               2000,
	MaxIdleConnDuration:           120 * time.Second,
	ReadTimeout:                   10 * time.Second,
	WriteTimeout:                  10 * time.Second,
	MaxConnWaitTimeout:            5 * time.Second,
	MaxConnDuration:               60 * time.Second,
	ReadBufferSize:                64 * 1024,
	WriteBufferSize:               64 * 1024,
	DisableHeaderNamesNormalizing: true,
	DisablePathNormalizing:        true,
}

type BenchStats struct {
	ops       uint64
	errors    uint64
	latencies []time.Duration
	mu        sync.Mutex
}

func (s *BenchStats) AddLatency(d time.Duration) {
	s.mu.Lock()
	s.latencies = append(s.latencies, d)
	s.mu.Unlock()
}

func (s *BenchStats) Percentiles() (p50, p95, p99 time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.latencies) == 0 {
		return 0, 0, 0
	}

	sorted := make([]time.Duration, len(s.latencies))
	copy(sorted, s.latencies)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	p50 = sorted[len(sorted)*50/100]
	p95 = sorted[len(sorted)*95/100]
	p99 = sorted[len(sorted)*99/100]
	return
}

func main() {
	rand.Seed(time.Now().UnixNano())
	runtime.GOMAXPROCS(runtime.NumCPU())

	fmt.Printf("Fasty Ultra-High Throughput Benchmark - %d CPU cores\n", runtime.NumCPU())
	fmt.Println("Optimized for maximum ops/sec with massive batches")
	fmt.Println(strings.Repeat("=", 80))

	setup()
	totalStart := time.Now()
	var totalOps uint64

	fmt.Println("\n[1] Single INSERT (baseline)")
	totalOps += benchSingleInserts(5000, 100)

	fmt.Println("\n[2] Batch INSERT via /query")
	for _, size := range []int{1000, 5000, 10000, 25000, 50000} {
		fmt.Printf("  batch size %d: ", size)
		totalOps += benchBatchInserts(200000, size, "/query")
	}

	fmt.Println("\n[3] Parallel /query")
	configs := []struct{ total, batch, workers int }{
		{500000, 5000, 20},
		{1000000, 10000, 50},
		{2000000, 25000, 100},
		{5000000, 50000, 200},
	}
	for _, c := range configs {
		fmt.Printf("  %dk rows, %d/batch, %d workers: ", c.total/1000, c.batch, c.workers)
		totalOps += benchParallelBatch(c.total, c.batch, c.workers)
	}

	fmt.Println("\n[4] Large payloads")
	for _, size := range []int{1024, 4096} {
		fmt.Printf("  %d bytes/row: ", size)
		totalOps += benchLargePayloads(5000, size)
	}

	fmt.Println("\n[5] Mixed read/write workload")
	fmt.Printf("  70%% write: ")
	totalOps += benchMixedWorkload(10000, 20, 0.7)
	fmt.Printf("  50%% write: ")
	totalOps += benchMixedWorkload(10000, 20, 0.5)
	fmt.Printf("  30%% write: ")
	totalOps += benchMixedWorkload(10000, 20, 0.3)

	fmt.Println("\n[6] Query performance")
	populateIndexedTable(50000)
	populateJoinTables(10000, 5)
	fmt.Printf("  index lookup: ")
	totalOps += benchIndexedQueries(2000, 20)
	fmt.Printf("  range query: ")
	totalOps += benchRangeQueries(500, 10)
	fmt.Printf("  order by + limit: ")
	totalOps += benchOrderByQueries(200, 5)
	fmt.Printf("  join query: ")
	totalOps += benchJoinQueries(1000, 5)

	fmt.Println("\n[7] Sustained load (10 seconds)")
	totalOps += benchSustainedLoad(10*time.Second, 100, 5000)

	fmt.Println("\n[8] Burst traffic (5 waves)")
	totalOps += benchBurstTraffic(5, 200000, 10000, 50)

	fmt.Println("\n[9] Latency distribution")
	fmt.Printf("  single inserts: ")
	totalOps += benchLatencyDistribution(5000, 50)
	fmt.Printf("  batch inserts: ")
	totalOps += benchBatchLatency(500, 500, 20)

	fmt.Println("\n[10] Ultra-high throughput")
	fmt.Printf("  100k batches x 50k ops: ")
	totalOps += benchUltraBatch(100, 50000, 200)
	fmt.Printf("  sustained 30s x 200 workers x 10k batch: ")
	totalOps += benchUltraSustained(30*time.Second, 200, 10000)

	totalElapsed := time.Since(totalStart)
	totalThroughput := float64(totalOps) / totalElapsed.Seconds()

	fmt.Println(strings.Repeat("=", 60))
	fmt.Printf("Total operations: %d\n", totalOps)
	fmt.Printf("Total time: %v\n", totalElapsed.Round(time.Second))
	fmt.Printf("Total throughput: %.0f ops/sec\n", totalThroughput)
	fmt.Println(strings.Repeat("=", 80))
	fmt.Println("NOTE: Run server with --memory flag for 2-3x higher throughput (no persistence)")
	fmt.Println("      ./server --memory &")
}

func setup() {
	query("drop table bench_stress")
	query("drop table bench_large")
	query("drop table bench_indexed")
	query("drop table bench_users")
	query("drop table bench_orders")
	query("create table bench_stress { id: int, name: string, value: int, data: string, score: float, active: bool }")
	query("create table bench_large { id: int, payload: string }")
	query("create table bench_indexed { id: int, email: string, category: string, ts: int }")
	query("create table bench_users { id: int, name: string, country: string }")
	query("create table bench_orders { id: int, user_id: int, amount: int, ts: int }")
	query("create index idx_email on bench_indexed (email)")
	query("create index idx_cat on bench_indexed (category)")
	query("create index idx_orders_user on bench_orders (user_id)")
}

func query(q string) (string, error) {
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)

	req.SetRequestURI(baseURL + "/query")
	req.Header.SetMethod("POST")
	req.Header.SetContentType("application/json")

	body, _ := json.Marshal(map[string]string{"query": q})
	req.SetBody(body)

	err := client.Do(req, resp)
	return string(resp.Body()), err
}

func batchInsert(table string, rows []map[string]interface{}, endpoint string) error {
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)

	req.SetRequestURI(baseURL + endpoint)
	req.Header.SetMethod("POST")
	req.Header.SetContentType("application/json")

	body, _ := json.Marshal(map[string]interface{}{"table": table, "rows": rows})
	req.SetBody(body)

	return client.Do(req, resp)
}

func benchSingleInserts(total, workers int) uint64 {
	var ops, errors uint64
	start := time.Now()
	perWorker := total / workers

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				id := wid*perWorker + i
				q := fmt.Sprintf(`insert bench_stress { id: %d, name: "u%d", value: %d, data: "test", score: %f, active: true }`,
					id, id, id*10, float64(id)*0.5)
				if _, err := query(q); err != nil {
					atomic.AddUint64(&errors, 1)
				} else {
					atomic.AddUint64(&ops, 1)
				}
			}
		}(w)
	}

	wg.Wait()
	elapsed := time.Since(start)
	fmt.Printf("%d ops in %v (%.0f ops/sec)\n", ops, elapsed.Round(time.Millisecond), float64(ops)/elapsed.Seconds())
	return ops
}

func benchBatchInserts(total, batchSize int, endpoint string) uint64 {
	var ops, errors uint64
	start := time.Now()
	numBatches := total / batchSize

	for b := 0; b < numBatches; b++ {
		rows := make([]map[string]interface{}, batchSize)
		for i := 0; i < batchSize; i++ {
			id := b*batchSize + i
			rows[i] = map[string]interface{}{
				"id": id, "name": fmt.Sprintf("u%d", id), "value": id * 10,
				"data": "benchmark", "score": float64(id) * 0.5, "active": id%2 == 0,
			}
		}
		if err := batchInsert("bench_stress", rows, endpoint); err != nil {
			atomic.AddUint64(&errors, 1)
		} else {
			atomic.AddUint64(&ops, uint64(batchSize))
		}
	}

	elapsed := time.Since(start)
	fmt.Printf("%d ops in %v (%.0f ops/sec)\n", ops, elapsed.Round(time.Millisecond), float64(ops)/elapsed.Seconds())
	return ops
}

func benchParallelBatch(total, batchSize, workers int) uint64 {
	var ops, errors uint64
	start := time.Now()
	batchesPerWorker := (total / batchSize) / workers

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			for b := 0; b < batchesPerWorker; b++ {
				rows := make([]map[string]interface{}, batchSize)
				base := (wid*batchesPerWorker + b) * batchSize
				for i := 0; i < batchSize; i++ {
					id := base + i
					rows[i] = map[string]interface{}{
						"id": id, "name": fmt.Sprintf("u%d", id), "value": id * 10,
						"data": "parallel test", "score": float64(id) * 0.5, "active": id%2 == 0,
					}
				}
				if err := batchInsert("bench_stress", rows, "/query"); err != nil {
					atomic.AddUint64(&errors, 1)
				} else {
					atomic.AddUint64(&ops, uint64(batchSize))
				}
			}
		}(w)
	}

	wg.Wait()
	elapsed := time.Since(start)
	fmt.Printf("%d ops in %v (%.0f ops/sec)\n", ops, elapsed.Round(time.Millisecond), float64(ops)/elapsed.Seconds())
	return ops
}

func benchLargePayloads(total, payloadSize int) uint64 {
	var ops uint64
	start := time.Now()
	payload := strings.Repeat("X", payloadSize)
	batchSize := 100

	for b := 0; b < total/batchSize; b++ {
		rows := make([]map[string]interface{}, batchSize)
		for i := 0; i < batchSize; i++ {
			rows[i] = map[string]interface{}{"id": b*batchSize + i, "payload": payload}
		}
		if err := batchInsert("bench_large", rows, "/query"); err == nil {
			atomic.AddUint64(&ops, uint64(batchSize))
		}
	}

	elapsed := time.Since(start)
	mbps := float64(ops) * float64(payloadSize) / elapsed.Seconds() / 1024 / 1024
	fmt.Printf("%d ops in %v (%.0f ops/sec, %.1f MB/sec)\n", ops, elapsed.Round(time.Millisecond), float64(ops)/elapsed.Seconds(), mbps)
	return ops
}

func benchMixedWorkload(total, workers int, writeRatio float64) uint64 {
	var writes, reads, errors uint64
	start := time.Now()
	perWorker := total / workers

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				if rand.Float64() < writeRatio {
					id := wid*perWorker + i + 10000000
					q := fmt.Sprintf(`insert bench_stress { id: %d, name: "m%d", value: %d, data: "mixed", score: 1.0, active: true }`, id, id, id)
					if _, err := query(q); err != nil {
						atomic.AddUint64(&errors, 1)
					} else {
						atomic.AddUint64(&writes, 1)
					}
				} else {
					q := fmt.Sprintf(`find bench_stress where id = %d`, rand.Intn(50000))
					if _, err := query(q); err != nil {
						atomic.AddUint64(&errors, 1)
					} else {
						atomic.AddUint64(&reads, 1)
					}
				}
			}
		}(w)
	}

	wg.Wait()
	elapsed := time.Since(start)
	total64 := writes + reads
	fmt.Printf("%d ops (%d w, %d r) in %v (%.0f ops/sec)\n", total64, writes, reads, elapsed.Round(time.Millisecond), float64(total64)/elapsed.Seconds())
	return total64
}

func populateIndexedTable(total int) {
	cats := []string{"electronics", "clothing", "books", "food", "toys", "sports", "home", "auto"}
	batchSize := 2500
	for b := 0; b < total/batchSize; b++ {
		rows := make([]map[string]interface{}, batchSize)
		for i := 0; i < batchSize; i++ {
			id := b*batchSize + i
			rows[i] = map[string]interface{}{
				"id": id, "email": fmt.Sprintf("u%d@test.com", id),
				"category": cats[id%len(cats)], "ts": time.Now().UnixNano(),
			}
		}
		batchInsert("bench_indexed", rows, "/query")
	}
	time.Sleep(300 * time.Millisecond)
}

func benchIndexedQueries(total, workers int) uint64 {
	var ops uint64
	start := time.Now()
	perWorker := total / workers

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				q := fmt.Sprintf(`find bench_indexed where email = "u%d@test.com"`, rand.Intn(50000))
				if _, err := query(q); err == nil {
					atomic.AddUint64(&ops, 1)
				}
			}
		}()
	}

	wg.Wait()
	elapsed := time.Since(start)
	fmt.Printf("%d queries in %v (%.0f q/sec)\n", ops, elapsed.Round(time.Millisecond), float64(ops)/elapsed.Seconds())
	return ops
}

func benchRangeQueries(total, workers int) uint64 {
	var ops uint64
	start := time.Now()
	cats := []string{"electronics", "clothing", "books", "food", "toys", "sports", "home", "auto"}
	perWorker := total / workers

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				q := fmt.Sprintf(`find bench_indexed where category = "%s" limit 50`, cats[rand.Intn(len(cats))])
				if _, err := query(q); err == nil {
					atomic.AddUint64(&ops, 1)
				}
			}
		}()
	}

	wg.Wait()
	elapsed := time.Since(start)
	fmt.Printf("%d queries in %v (%.0f q/sec)\n", ops, elapsed.Round(time.Millisecond), float64(ops)/elapsed.Seconds())
	return ops
}

func benchOrderByQueries(total, workers int) uint64 {
	var ops uint64
	start := time.Now()
	perWorker := total / workers

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				if _, err := query(`SELECT * FROM bench_indexed ORDER BY ts DESC LIMIT 50`); err == nil {
					atomic.AddUint64(&ops, 1)
				}
			}
		}()
	}

	wg.Wait()
	elapsed := time.Since(start)
	fmt.Printf("%d queries in %v (%.0f q/sec)\n", ops, elapsed.Round(time.Millisecond), float64(ops)/elapsed.Seconds())
	return ops
}

func benchSustainedLoad(duration time.Duration, workers, batchSize int) uint64 {
	var ops uint64
	start := time.Now()
	deadline := start.Add(duration)

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			batch := 0
			for time.Now().Before(deadline) {
				rows := make([]map[string]interface{}, batchSize)
				base := wid*1000000 + batch*batchSize
				for i := 0; i < batchSize; i++ {
					id := base + i
					rows[i] = map[string]interface{}{
						"id": id, "name": fmt.Sprintf("s%d", id), "value": id,
						"data": "sustained", "score": 1.0, "active": true,
					}
				}
				if err := batchInsert("bench_stress", rows, "/query"); err == nil {
					atomic.AddUint64(&ops, uint64(batchSize))
				}
				batch++
			}
		}(w)
	}

	wg.Wait()
	elapsed := time.Since(start)
	fmt.Printf("%d ops in %v (%.0f ops/sec avg)\n", ops, elapsed.Round(time.Millisecond), float64(ops)/elapsed.Seconds())
	return ops
}

func benchBurstTraffic(waves, opsPerWave, batchSize, workers int) uint64 {
	var totalOps uint64
	start := time.Now()

	for wave := 0; wave < waves; wave++ {
		waveStart := time.Now()
		var ops uint64
		batchesPerWorker := (opsPerWave / batchSize) / workers

		var wg sync.WaitGroup
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func(wid int) {
				defer wg.Done()
				for b := 0; b < batchesPerWorker; b++ {
					rows := make([]map[string]interface{}, batchSize)
					base := wave*1000000 + wid*100000 + b*batchSize
					for i := 0; i < batchSize; i++ {
						id := base + i
						rows[i] = map[string]interface{}{
							"id": id, "name": fmt.Sprintf("b%d", id), "value": id,
							"data": "burst", "score": 1.0, "active": true,
						}
					}
					if err := batchInsert("bench_stress", rows, "/query"); err == nil {
						atomic.AddUint64(&ops, uint64(batchSize))
					}
				}
			}(w)
		}
		wg.Wait()

		waveElapsed := time.Since(waveStart)
		fmt.Printf("  wave %d: %d ops in %v (%.0f ops/sec)\n", wave+1, ops, waveElapsed.Round(time.Millisecond), float64(ops)/waveElapsed.Seconds())
		atomic.AddUint64(&totalOps, ops)
		time.Sleep(50 * time.Millisecond)
	}

	elapsed := time.Since(start)
	fmt.Printf("  total: %d ops in %v (%.0f ops/sec avg)\n", totalOps, elapsed.Round(time.Millisecond), float64(totalOps)/elapsed.Seconds())
	return totalOps
}

func benchLatencyDistribution(total, workers int) uint64 {
	stats := &BenchStats{latencies: make([]time.Duration, 0, total)}
	perWorker := total / workers

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				id := wid*perWorker + i + 50000000
				q := fmt.Sprintf(`insert bench_stress { id: %d, name: "l%d", value: %d, data: "lat", score: true }`, id, id, id)
				opStart := time.Now()
				if _, err := query(q); err == nil {
					atomic.AddUint64(&stats.ops, 1)
					stats.AddLatency(time.Since(opStart))
				}
			}
		}(w)
	}

	wg.Wait()
	p50, p95, p99 := stats.Percentiles()
	fmt.Printf("%d ops, p50=%v p95=%v p99=%v\n", stats.ops, p50.Round(time.Microsecond), p95.Round(time.Microsecond), p99.Round(time.Microsecond))
	return stats.ops
}

func benchBatchLatency(numBatches, batchSize, workers int) uint64 {
	stats := &BenchStats{latencies: make([]time.Duration, 0, numBatches)}
	perWorker := numBatches / workers

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			for b := 0; b < perWorker; b++ {
				rows := make([]map[string]interface{}, batchSize)
				base := wid*1000000 + b*batchSize + 60000000
				for i := 0; i < batchSize; i++ {
					id := base + i
					rows[i] = map[string]interface{}{
						"id": id, "name": fmt.Sprintf("bl%d", id), "value": id,
						"data": "batchlat", "score": 1.0, "active": true,
					}
				}
				opStart := time.Now()
				if err := batchInsert("bench_stress", rows, "/query"); err == nil {
					atomic.AddUint64(&stats.ops, uint64(batchSize))
					stats.AddLatency(time.Since(opStart))
				}
			}
		}(w)
	}

	wg.Wait()
	p50, p95, p99 := stats.Percentiles()
	fmt.Printf("%d ops, p50=%v p95=%v p99=%v\n", stats.ops, p50.Round(time.Microsecond), p95.Round(time.Microsecond), p99.Round(time.Microsecond))
	return stats.ops
}

func populateJoinTables(totalUsers, ordersPerUser int) {
	batchSize := 1000
	for b := 0; b < totalUsers/batchSize; b++ {
		rows := make([]map[string]interface{}, batchSize)
		for i := 0; i < batchSize; i++ {
			id := b*batchSize + i
			rows[i] = map[string]interface{}{
				"id": id, "name": fmt.Sprintf("User%d", id),
				"country": "USA",
			}
		}
		batchInsert("bench_users", rows, "/query")
	}

	totalOrders := totalUsers * ordersPerUser
	for b := 0; b < totalOrders/batchSize; b++ {
		rows := make([]map[string]interface{}, batchSize)
		for i := 0; i < batchSize; i++ {
			id := b*batchSize + i
			userID := rand.Intn(totalUsers)
			rows[i] = map[string]interface{}{
				"id": id, "user_id": userID,
				"amount": rand.Intn(1000), "ts": time.Now().UnixNano(),
			}
		}
		batchInsert("bench_orders", rows, "/query")
	}
	time.Sleep(500 * time.Millisecond)
}

func benchJoinQueries(total, workers int) uint64 {
	var ops uint64
	start := time.Now()
	perWorker := total / workers

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				q := `SELECT * FROM bench_orders JOIN bench_users ON bench_orders.user_id = bench_users.id LIMIT 10`
				if _, err := query(q); err == nil {
					atomic.AddUint64(&ops, 1)
				}
			}
		}()
	}

	wg.Wait()
	elapsed := time.Since(start)
	fmt.Printf("%d queries in %v (%.0f q/sec)\n", ops, elapsed.Round(time.Millisecond), float64(ops)/elapsed.Seconds())
	return ops
}

func benchUltraBatch(numBatches, batchSize, workers int) uint64 {
	var ops uint64
	start := time.Now()
	batchesPerWorker := numBatches / workers

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			for b := 0; b < batchesPerWorker; b++ {
				rows := make([]map[string]interface{}, batchSize)
				base := wid*10000000 + b*batchSize
				for i := 0; i < batchSize; i++ {
					id := base + i
					rows[i] = map[string]interface{}{
						"id": id, "name": fmt.Sprintf("ultra%d", id), "value": id,
						"data": "ultra", "score": 1.0, "active": true,
					}
				}
				if err := batchInsert("bench_stress", rows, "/query"); err == nil {
					atomic.AddUint64(&ops, uint64(batchSize))
				}
			}
		}(w)
	}

	wg.Wait()
	elapsed := time.Since(start)
	fmt.Printf("%d ops in %v (%.0f ops/sec)\n", ops, elapsed.Round(time.Millisecond), float64(ops)/elapsed.Seconds())
	return ops
}

func benchUltraSustained(duration time.Duration, workers, batchSize int) uint64 {
	var ops uint64
	start := time.Now()
	deadline := start.Add(duration)

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			batch := 0
			for time.Now().Before(deadline) {
				rows := make([]map[string]interface{}, batchSize)
				base := wid*100000000 + batch*batchSize
				for i := 0; i < batchSize; i++ {
					id := base + i
					rows[i] = map[string]interface{}{
						"id": id, "name": fmt.Sprintf("us%d", id), "value": id,
						"data": "ultrasustained", "score": 1.0, "active": true,
					}
				}
				if err := batchInsert("bench_stress", rows, "/query"); err == nil {
					atomic.AddUint64(&ops, uint64(batchSize))
				}
				batch++
			}
		}(w)
	}

	wg.Wait()
	elapsed := time.Since(start)
	fmt.Printf("%d ops in %v (%.0f ops/sec avg)\n", ops, elapsed.Round(time.Millisecond), float64(ops)/elapsed.Seconds())
	return ops
}
