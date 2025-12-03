package main

import (
	"bufio"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/valyala/fasthttp"
)

const baseURL = "http://localhost:1337"

var client = &fasthttp.Client{
	MaxConnsPerHost:               500,
	ReadTimeout:                   60 * time.Second,
	WriteTimeout:                  60 * time.Second,
	MaxIdleConnDuration:           120 * time.Second,
	ReadBufferSize:                128 * 1024,
	WriteBufferSize:               128 * 1024,
	DisableHeaderNamesNormalizing: true,
	DisablePathNormalizing:        true,
}

func query(q string) error {
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)

	req.SetRequestURI(baseURL + "/query")
	req.Header.SetMethod("POST")
	req.Header.SetContentType("application/json")

	body := fmt.Sprintf(`{"query": %q}`, q)
	req.SetBodyString(body)

	err := client.Do(req, resp)
	if err != nil {
		return err
	}

	if resp.StatusCode() != 200 {
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode(), string(resp.Body()))
	}

	return nil
}

func batchInsert(table string, columns []string, rows [][]interface{}) error {
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)

	req.SetRequestURI(baseURL + "/query")
	req.Header.SetMethod("POST")
	req.Header.SetContentType("application/json")

	body := fmt.Sprintf(`{"table": %q, "rows": %s}`, table, formatRowsAsObjects(columns, rows))
	req.SetBodyString(body)

	err := client.Do(req, resp)
	if err != nil {
		return err
	}

	if resp.StatusCode() != 200 {
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode(), string(resp.Body()))
	}

	return nil
}

func formatRowsAsObjects(columns []string, rows [][]interface{}) string {
	var sb strings.Builder
	sb.Grow(len(rows) * len(columns) * 50) // Pre-allocate rough estimate

	sb.WriteByte('[')
	for i, row := range rows {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteByte('{')

		for j, val := range row {
			if j >= len(columns) {
				continue
			}
			if j > 0 {
				sb.WriteString(", ")
			}

			sb.WriteByte('"')
			sb.WriteString(columns[j])
			sb.WriteString(`": `)

			switch v := val.(type) {
			case string:
				sb.WriteByte('"')
				sb.WriteString(v)
				sb.WriteByte('"')
			case int:
				sb.WriteString(strconv.Itoa(v))
			case int64:
				sb.WriteString(strconv.FormatInt(v, 10))
			case float64:
				sb.WriteString(strconv.FormatFloat(v, 'f', 2, 64))
			default:
				sb.WriteByte('"')
				sb.WriteString(fmt.Sprintf("%v", v))
				sb.WriteByte('"')
			}
		}
		sb.WriteByte('}')
	}
	sb.WriteByte(']')

	return sb.String()
}

func sanitizeColumnName(name string) string {
	colName := strings.ToLower(name)

	colName = strings.ReplaceAll(colName, " ", "_")
	colName = strings.ReplaceAll(colName, "-", "_")
	colName = strings.ReplaceAll(colName, "(", "_")
	colName = strings.ReplaceAll(colName, ")", "_")
	colName = strings.ReplaceAll(colName, "[", "_")
	colName = strings.ReplaceAll(colName, "]", "_")
	colName = strings.ReplaceAll(colName, ".", "_")
	colName = strings.ReplaceAll(colName, ",", "_")
	colName = strings.ReplaceAll(colName, ":", "_")
	colName = strings.ReplaceAll(colName, ";", "_")
	colName = strings.ReplaceAll(colName, "'", "_")
	colName = strings.ReplaceAll(colName, "\"", "_")
	colName = strings.ReplaceAll(colName, "/", "_")
	colName = strings.ReplaceAll(colName, "\\", "_")
	colName = strings.ReplaceAll(colName, "|", "_")
	colName = strings.ReplaceAll(colName, "@", "_")
	colName = strings.ReplaceAll(colName, "#", "_")
	colName = strings.ReplaceAll(colName, "$", "_")
	colName = strings.ReplaceAll(colName, "%", "_")
	colName = strings.ReplaceAll(colName, "^", "_")
	colName = strings.ReplaceAll(colName, "&", "_")
	colName = strings.ReplaceAll(colName, "*", "_")
	colName = strings.ReplaceAll(colName, "+", "_")
	colName = strings.ReplaceAll(colName, "=", "_")
	colName = strings.ReplaceAll(colName, "<", "_")
	colName = strings.ReplaceAll(colName, ">", "_")
	colName = strings.ReplaceAll(colName, "?", "_")
	colName = strings.ReplaceAll(colName, "`", "_")
	colName = strings.ReplaceAll(colName, "~", "_")

	colName = strings.Trim(colName, "_")

	if colName == "" {
		colName = "col"
	}

	if colName[0] >= '0' && colName[0] <= '9' {
		colName = "col_" + colName
	}

	return colName
}

func createTable(tableName string, headers []string, sampleRows [][]string) error {
	var columns []string

	sanitizedColumns := make([]string, len(headers))
	for i, header := range headers {
		colName := sanitizeColumnName(header)
		sanitizedColumns[i] = colName
		columns = append(columns, fmt.Sprintf("%s string", colName))
	}

	queryStr := fmt.Sprintf("create table %s (%s)", tableName, strings.Join(columns, ", "))
	fmt.Printf("Creating table: %s\n", queryStr)

	return query(queryStr)
}

type batchJob struct {
	rows [][]interface{}
}

func importCSV(filename string, batchSize int, numWorkers int) error {
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()

	bufferedReader := bufio.NewReaderSize(file, 64*1024*1024) // 64MB buffer
	reader := csv.NewReader(bufferedReader)
	reader.Comma = ','
	reader.LazyQuotes = true
	reader.TrimLeadingSpace = true
	reader.ReuseRecord = true
	reader.FieldsPerRecord = -1 // Allow variable number of fields

	rawHeaders, err := reader.Read()
	if err != nil {
		return fmt.Errorf("failed to read headers: %v", err)
	}

	headers := make([]string, len(rawHeaders))
	copy(headers, rawHeaders)

	fmt.Printf("Detected %d columns: %v\n", len(headers), headers)

	hasHeaders := true
	for _, header := range headers {
		if header == "" {
			continue
		}
		if _, err := strconv.Atoi(header); err == nil {
			hasHeaders = false
			break
		}
		if _, err := strconv.ParseFloat(header, 64); err == nil {
			hasHeaders = false
			break
		}
	}

	if !hasHeaders {
		fmt.Println("No valid headers detected, generating column names")
		for i := range headers {
			headers[i] = fmt.Sprintf("col_%d", i+1)
		}
	}

	var columns []string
	sanitizedColumns := make([]string, len(headers))
	for i, header := range headers {
		colName := sanitizeColumnName(header)
		sanitizedColumns[i] = colName
		columns = append(columns, fmt.Sprintf("%s: %s", colName, "string"))
	}

	sampleRows := make([][]string, 0, 5)
	for i := 0; i < 5; i++ {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read sample row: %v", err)
		}

		sampleRows = append(sampleRows, row)
	}

	tableName := "transactions"
	if err := createTable(tableName, headers, sampleRows); err != nil {
		return fmt.Errorf("failed to create table: %v", err)
	}

	file.Seek(0, 0)
	bufferedReader.Reset(file)
	reader = csv.NewReader(bufferedReader)
	reader.LazyQuotes = true
	reader.TrimLeadingSpace = true
	reader.ReuseRecord = true

	_, err = reader.Read()
	if err != nil {
		return fmt.Errorf("failed to skip headers: %v", err)
	}

	if numWorkers > 32 {
		numWorkers = 32
	}

	fmt.Printf("Using %d concurrent workers with batch size %d\n", numWorkers, batchSize)

	batchChan := make(chan *batchJob, numWorkers*2)
	errorChan := make(chan error, numWorkers)
	doneChan := make(chan struct{})

	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for job := range batchChan {
				if err := batchInsert(tableName, sanitizedColumns, job.rows); err != nil {
					select {
					case errorChan <- fmt.Errorf("worker %d failed: %v", workerID, err):
					default:
					}
					return
				}
			}
		}(i)
	}

	go func() {
		wg.Wait()
		close(doneChan)
	}()

	var currentBatch [][]interface{}
	totalRows := int64(0)
	startTime := time.Now()
	lastReport := startTime

	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Warning: skipping malformed row: %v", err)
			continue
		}

		if len(row) != len(headers) {
			log.Printf("Warning: row has %d columns, expected %d, skipping", len(row), len(headers))
			continue
		}

		var convertedRow []interface{}
		for _, value := range row {
			convertedRow = append(convertedRow, value)
		}

		currentBatch = append(currentBatch, convertedRow)

		if len(currentBatch) >= batchSize {
			job := &batchJob{rows: make([][]interface{}, len(currentBatch))}
			copy(job.rows, currentBatch)

			select {
			case batchChan <- job:
			case err := <-errorChan:
				close(batchChan)
				return err
			}

			atomic.AddInt64(&totalRows, int64(len(currentBatch)))

			now := time.Now()
			if now.Sub(lastReport) >= time.Second {
				elapsed := now.Sub(startTime)
				rate := float64(atomic.LoadInt64(&totalRows)) / elapsed.Seconds()
				fmt.Printf("Imported %d rows (%.0f rows/sec)\n", atomic.LoadInt64(&totalRows), rate)
				lastReport = now
			}

			currentBatch = currentBatch[:0]
		}
	}

	if len(currentBatch) > 0 {
		job := &batchJob{rows: make([][]interface{}, len(currentBatch))}
		copy(job.rows, currentBatch)

		select {
		case batchChan <- job:
		case err := <-errorChan:
			close(batchChan)
			return err
		}

		atomic.AddInt64(&totalRows, int64(len(currentBatch)))
	}

	close(batchChan)

	select {
	case err := <-errorChan:
		return err
	case <-doneChan:
	}

	elapsed := time.Since(startTime)
	rate := float64(atomic.LoadInt64(&totalRows)) / elapsed.Seconds()
	fmt.Printf("Import complete! Total: %d rows in %v (%.0f rows/sec)\n",
		atomic.LoadInt64(&totalRows), elapsed, rate)

	return nil
}

func main() {
	batchSizeFlag := flag.Int("batch-size", 10000, "Number of rows per batch")
	workersFlag := flag.Int("workers", runtime.NumCPU()*2, "Number of concurrent workers")
	flag.Parse()

	if len(flag.Args()) != 1 {
		fmt.Fprintf(os.Stderr, "Usage: %s [flags] <csv_file>\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Flags:\n")
		flag.PrintDefaults()
		os.Exit(1)
	}

	filename := flag.Args()[0]

	fmt.Printf("Starting CSV import from: %s\n", filename)
	fmt.Printf("Configuration: batch-size=%d, workers=%d\n", *batchSizeFlag, *workersFlag)
	fmt.Println("Make sure fasty server is running on http://localhost:1337")

	if err := importCSV(filename, *batchSizeFlag, *workersFlag); err != nil {
		log.Fatalf("Import failed: %v", err)
	}
}
