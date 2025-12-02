package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	json "github.com/goccy/go-json"
	"github.com/valyala/fasthttp"

	"github.com/r-ef/fasty/src/cmd/engine"
)

var inMemoryMode = flag.Bool("memory", false, "Run in ultra-fast in-memory mode (no persistence)")

type QueryRequest struct {
	Query string `json:"query"`
}

type BatchRequest struct {
	Table string                   `json:"table"`
	Rows  []map[string]interface{} `json:"rows"`
}

type Response struct {
	Success bool   `json:"success"`
	Data    string `json:"data,omitempty"`
	Error   string `json:"error,omitempty"`
}

type BatchResponse struct {
	Success  bool   `json:"success"`
	Inserted int    `json:"inserted"`
	Error    string `json:"error,omitempty"`
}

var db *engine.RelationalDB

var (
	healthResponse   = []byte(`{"success":true,"data":"ok"}`)
	emptyQueryError  = []byte(`{"success":false,"error":"query cannot be empty"}`)
	emptyBodyError   = []byte(`{"success":false,"error":"empty request body"}`)
	methodNotAllowed = []byte(`{"success":false,"error":"only POST method allowed"}`)
	contentTypeJSON  = []byte("application/json")
)

func main() {
	flag.Parse()

	runtime.GOMAXPROCS(runtime.NumCPU())

	var err error
	if *inMemoryMode {
		log.Println("🚀 Starting in ULTRA-FAST in-memory mode (no persistence)")
		db, err = engine.NewInMemoryDB()
	} else {
		db, err = engine.NewRelationalDB("./data")
	}
	if err != nil {
		log.Fatalf("failed to open database: %v", err)
	}

	server := &fasthttp.Server{
		Handler:                       requestHandler,
		Name:                          "fasty",
		ReadTimeout:                   30 * time.Second,
		WriteTimeout:                  60 * time.Second,
		IdleTimeout:                   120 * time.Second,
		MaxRequestBodySize:            100 << 20,
		DisableKeepalive:              false,
		TCPKeepalive:                  true,
		TCPKeepalivePeriod:            60 * time.Second,
		ReduceMemoryUsage:             false,
		GetOnly:                       false,
		DisableHeaderNamesNormalizing: true,
		NoDefaultServerHeader:         true,
		NoDefaultDate:                 true,
		NoDefaultContentType:          true,
		Concurrency:                   256 * 1024,
	}

	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		<-sigChan

		log.Println("shutting down...")
		server.Shutdown()

		if err := db.Close(); err != nil {
			log.Printf("error closing database: %v", err)
		}
		os.Exit(0)
	}()

	log.Printf("⚡ fasty database server started on :8000")
	log.Printf("   using %d CPU cores with fasthttp", runtime.NumCPU())

	if err := server.ListenAndServe(":8000"); err != nil {
		log.Fatal("error starting server: ", err)
	}
}

var turboInserters = make(map[string]*engine.HighSpeedInserter)
var turboMu sync.Mutex

func requestHandler(ctx *fasthttp.RequestCtx) {
	path := string(ctx.Path())

	switch path {
	case "/query":
		handleTurbo(ctx)
	case "/health":
		handleHealth(ctx)
	case "/stats":
		handleStats(ctx)
	default:
		ctx.SetStatusCode(fasthttp.StatusNotFound)
		ctx.SetBody([]byte(`{"success":false,"error":"not found"}`))
	}
}

func handleTurbo(ctx *fasthttp.RequestCtx) {
	ctx.SetContentTypeBytes(contentTypeJSON)

	if !ctx.IsPost() {
		ctx.SetStatusCode(fasthttp.StatusMethodNotAllowed)
		ctx.SetBody(methodNotAllowed)
		return
	}

	body := ctx.PostBody()
	if len(body) == 0 {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.SetBody(emptyBodyError)
		return
	}

	var req BatchRequest
	if err := json.Unmarshal(body, &req); err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		writeError(ctx, "invalid JSON: "+err.Error())
		return
	}

	if req.Table == "" || len(req.Rows) == 0 {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		writeError(ctx, "table and rows required")
		return
	}

	turboMu.Lock()
	inserter, exists := turboInserters[req.Table]
	if !exists {
		var err error
		inserter, err = db.NewHighSpeedInserter(req.Table)
		if err != nil {
			turboMu.Unlock()
			ctx.SetStatusCode(fasthttp.StatusBadRequest)
			writeError(ctx, err.Error())
			return
		}
		turboInserters[req.Table] = inserter
	}
	turboMu.Unlock()

	inserter.InsertMaps(req.Rows)

	resp := BatchResponse{Success: true, Inserted: len(req.Rows)}
	data, _ := json.Marshal(resp)
	ctx.SetBody(data)
}

func handleHealth(ctx *fasthttp.RequestCtx) {
	ctx.SetContentTypeBytes(contentTypeJSON)
	ctx.SetBody(healthResponse)
}

func handleStats(ctx *fasthttp.RequestCtx) {
	ctx.SetContentTypeBytes(contentTypeJSON)

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	dbStats := db.Stats()

	resp := map[string]interface{}{
		"success": true,
		"data": map[string]interface{}{
			"goroutines":    runtime.NumGoroutine(),
			"heap_alloc_mb": m.HeapAlloc / 1024 / 1024,
			"heap_sys_mb":   m.HeapSys / 1024 / 1024,
			"num_gc":        m.NumGC,
			"cpu_cores":     runtime.NumCPU(),
			"db":            dbStats,
		},
	}

	data, _ := json.Marshal(resp)
	ctx.SetBody(data)
}

func writeError(ctx *fasthttp.RequestCtx, msg string) {
	resp := Response{Success: false, Error: msg}
	data, _ := json.Marshal(resp)
	ctx.SetBody(data)
}

func writeSuccess(ctx *fasthttp.RequestCtx, data string) {
	resp := Response{Success: true, Data: data}
	result, _ := json.Marshal(resp)
	ctx.SetBody(result)
}
