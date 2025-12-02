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
	MaxConnsPerHost:     500,
	MaxIdleConnDuration: 60 * time.Second,
	ReadTimeout:         30 * time.Second,
	WriteTimeout:        30 * time.Second,
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

	fmt.Printf("Fasty Benchmark - %d CPU cores\n", runtime.NumCPU())
	fmt.Println(strings.Repeat("=", 60))

	setup()
	totalStart := time.Now()

	fmt.Println("\n[1] Single INSERT (baseline)")
	benchSingleInserts(5000, 100)

	fmt.Println("\n[2] Batch INSERT via /batch")
	for _, size := range []int{100, 500, 1000, 5000} {
		fmt.Printf("  batch size %d: ", size)
		benchBatchInserts(50000, size, "/batch")
	}

	fmt.Println("\n[3] Batch INSERT via /turbo (async)")
	for _, size := range []int{100, 500, 1000, 5000} {
		fmt.Printf("  batch size %d: ", size)
		benchBatchInserts(100000, size, "/turbo")
	}

	fmt.Println("\n[4] Parallel /turbo")
	configs := []struct{ total, batch, workers int }{
		{200000, 1000, 10},
		{500000, 2000, 20},
		{1000000, 5000, 50},
	}
	for _, c := range configs {
		fmt.Printf("  %dk rows, %d/batch, %d workers: ", c.total/1000, c.batch, c.workers)
		benchParallelBatch(c.total, c.batch, c.workers)
	}

	fmt.Println("\n[5] Large payloads")
	for _, size := range []int{1024, 4096} {
		fmt.Printf("  %d bytes/row: ", size)
		benchLargePayloads(5000, size)
	}

	fmt.Println("\n[6] Mixed read/write workload")
	fmt.Printf("  70%% write: ")
	benchMixedWorkload(10000, 20, 0.7)
	fmt.Printf("  50%% write: ")
	benchMixedWorkload(10000, 20, 0.5)
	fmt.Printf("  30%% write: ")
	benchMixedWorkload(10000, 20, 0.3)

	fmt.Println("\n[7] Query performance")
	populateIndexedTable(50000)
	fmt.Printf("  index lookup: ")
	benchIndexedQueries(2000, 20)
	fmt.Printf("  range query: ")
	benchRangeQueries(500, 10)
	fmt.Printf("  order by + limit: ")
	benchOrderByQueries(200, 5)

	fmt.Println("\n[8] Sustained load (10 seconds)")
	benchSustainedLoad(10*time.Second, 50, 500)

	fmt.Println("\n[9] Burst traffic (5 waves)")
	benchBurstTraffic(5, 50000, 1000, 20)

	fmt.Println("\n[10] Latency distribution")
	fmt.Printf("  single inserts: ")
	benchLatencyDistribution(5000, 50)
	fmt.Printf("  batch inserts: ")
	benchBatchLatency(500, 500, 20)

	fmt.Println(strings.Repeat("=", 60))
	fmt.Printf("Total time: %v\n", time.Since(totalStart).Round(time.Second))
}

func setup() {
	query("drop table bench_stress")
	query("drop table bench_large")
	query("drop table bench_indexed")
	query("create table bench_stress { id: int, name: string, value: int, data: string, score: float, active: bool }")
	query("create table bench_large { id: int, payload: string }")
	query("create table bench_indexed { id: int, email: string, category: string, ts: int }")
	query("create index idx_email on bench_indexed (email)")
	query("create index idx_cat on bench_indexed (category)")
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

func benchSingleInserts(total, workers int) {
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
}

func benchBatchInserts(total, batchSize int, endpoint string) {
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
}

func benchParallelBatch(total, batchSize, workers int) {
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
				if err := batchInsert("bench_stress", rows, "/turbo"); err != nil {
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
}

func benchLargePayloads(total, payloadSize int) {
	var ops uint64
	start := time.Now()
	payload := strings.Repeat("X", payloadSize)
	batchSize := 100

	for b := 0; b < total/batchSize; b++ {
		rows := make([]map[string]interface{}, batchSize)
		for i := 0; i < batchSize; i++ {
			rows[i] = map[string]interface{}{"id": b*batchSize + i, "payload": payload}
		}
		if err := batchInsert("bench_large", rows, "/turbo"); err == nil {
			atomic.AddUint64(&ops, uint64(batchSize))
		}
	}

	elapsed := time.Since(start)
	mbps := float64(ops) * float64(payloadSize) / elapsed.Seconds() / 1024 / 1024
	fmt.Printf("%d ops in %v (%.0f ops/sec, %.1f MB/sec)\n", ops, elapsed.Round(time.Millisecond), float64(ops)/elapsed.Seconds(), mbps)
}

func benchMixedWorkload(total, workers int, writeRatio float64) {
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
		batchInsert("bench_indexed", rows, "/turbo")
	}
	time.Sleep(300 * time.Millisecond)
}

func benchIndexedQueries(total, workers int) {
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
}

func benchRangeQueries(total, workers int) {
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
}

func benchOrderByQueries(total, workers int) {
	var ops uint64
	start := time.Now()
	perWorker := total / workers

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				if _, err := query(`find bench_indexed order by ts desc limit 50`); err == nil {
					atomic.AddUint64(&ops, 1)
				}
			}
		}()
	}

	wg.Wait()
	elapsed := time.Since(start)
	fmt.Printf("%d queries in %v (%.0f q/sec)\n", ops, elapsed.Round(time.Millisecond), float64(ops)/elapsed.Seconds())
}

func benchSustainedLoad(duration time.Duration, workers, batchSize int) {
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
				if err := batchInsert("bench_stress", rows, "/turbo"); err == nil {
					atomic.AddUint64(&ops, uint64(batchSize))
				}
				batch++
			}
		}(w)
	}

	wg.Wait()
	elapsed := time.Since(start)
	fmt.Printf("%d ops in %v (%.0f ops/sec avg)\n", ops, elapsed.Round(time.Millisecond), float64(ops)/elapsed.Seconds())
}

func benchBurstTraffic(waves, opsPerWave, batchSize, workers int) {
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
					if err := batchInsert("bench_stress", rows, "/turbo"); err == nil {
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
}

func benchLatencyDistribution(total, workers int) {
	stats := &BenchStats{latencies: make([]time.Duration, 0, total)}
	start := time.Now()
	perWorker := total / workers

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				id := wid*perWorker + i + 50000000
				q := fmt.Sprintf(`insert bench_stress { id: %d, name: "l%d", value: %d, data: "lat", score: 1.0, active: true }`, id, id, id)
				opStart := time.Now()
				if _, err := query(q); err == nil {
					atomic.AddUint64(&stats.ops, 1)
					stats.AddLatency(time.Since(opStart))
				}
			}
		}(w)
	}

	wg.Wait()
	elapsed := time.Since(start)
	p50, p95, p99 := stats.Percentiles()
	fmt.Printf("%d ops, p50=%v p95=%v p99=%v\n", stats.ops, p50.Round(time.Microsecond), p95.Round(time.Microsecond), p99.Round(time.Microsecond))
	_ = elapsed
}

func benchBatchLatency(numBatches, batchSize, workers int) {
	stats := &BenchStats{latencies: make([]time.Duration, 0, numBatches)}
	start := time.Now()
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
				if err := batchInsert("bench_stress", rows, "/turbo"); err == nil {
					atomic.AddUint64(&stats.ops, uint64(batchSize))
					stats.AddLatency(time.Since(opStart))
				}
			}
		}(w)
	}

	wg.Wait()
	elapsed := time.Since(start)
	p50, p95, p99 := stats.Percentiles()
	fmt.Printf("%d ops, p50=%v p95=%v p99=%v\n", stats.ops, p50.Round(time.Microsecond), p95.Round(time.Microsecond), p99.Round(time.Microsecond))
	_ = elapsed
}
