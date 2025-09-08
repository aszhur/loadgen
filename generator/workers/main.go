package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"
)

const (
	defaultControlPlaneURL = "http://loadgen-control-plane:8080"
	defaultWorkerID        = ""
	defaultPort            = 8080
	defaultMetricsPort     = 9090
	defaultPollInterval    = 30 * time.Second
	defaultBatchSize       = 1000
	defaultFlushInterval   = 5 * time.Second
)

// Simplified metrics tracking (replace with actual Prometheus when available)
var (
	linesEmittedCount = make(map[string]int64)
	bytesEmittedCount = make(map[string]int64)
	httpErrorCount    = make(map[string]int64)
	metricsLock       sync.RWMutex
)

// WorkerConfig holds the worker configuration
type WorkerConfig struct {
	WorkerID         string
	ControlPlaneURL  string
	Port             int
	MetricsPort      int
	PollInterval     time.Duration
	BatchSize        int
	FlushInterval    time.Duration
}

// Assignment represents the current work assignment from control plane
type Assignment struct {
	WorkerID    string   `json:"worker_id"`
	PodName     string   `json:"pod_name"`
	Namespace   string   `json:"namespace"`
	Families    []string `json:"families"`
	Multiplier  float64  `json:"multiplier"`
	BurstFactor float64  `json:"burst_factor"`
	AssignedAt  time.Time `json:"assigned_at"`
}

// Recipe represents a loaded metric family recipe (simplified)
type Recipe struct {
	FamilyID   string                 `json:"family_id"`
	MetricName string                 `json:"metric_name"`
	Schema     map[string]interface{} `json:"schema"`
	Statistics map[string]interface{} `json:"statistics"`
	Temporal   map[string]interface{} `json:"temporal"`
}

// WavefrontSynthesizer generates Wavefront lines (simplified implementation)
type WavefrontSynthesizer struct {
	recipe      *Recipe
	rng         *rand.Rand
	metricName  string
	isDelat     bool
	sources     []string
	tags        map[string][]string
}

// LoadWorker represents a single worker pod that emits synthetic traffic
type LoadWorker struct {
	config        *WorkerConfig
	assignment    *Assignment
	synthesizers  map[string]*WavefrontSynthesizer
	httpClients   []*http.Client
	batchBuffer   *BatchBuffer
	mu            sync.RWMutex
	stopChan      chan struct{}
	wg            sync.WaitGroup
}

// BatchBuffer accumulates lines before sending
type BatchBuffer struct {
	lines     []string
	totalSize int
	mu        sync.Mutex
	maxSize   int
	maxLines  int
}

func NewBatchBuffer(maxLines int, maxSizeBytes int) *BatchBuffer {
	return &BatchBuffer{
		lines:    make([]string, 0, maxLines),
		maxLines: maxLines,
		maxSize:  maxSizeBytes,
	}
}

func (bb *BatchBuffer) Add(line string) bool {
	bb.mu.Lock()
	defer bb.mu.Unlock()

	if len(bb.lines) >= bb.maxLines || bb.totalSize+len(line) > bb.maxSize {
		return false // Buffer full
	}

	bb.lines = append(bb.lines, line)
	bb.totalSize += len(line) + 1 // +1 for newline
	return true
}

func (bb *BatchBuffer) Flush() []string {
	bb.mu.Lock()
	defer bb.mu.Unlock()

	if len(bb.lines) == 0 {
		return nil
	}

	result := make([]string, len(bb.lines))
	copy(result, bb.lines)
	
	bb.lines = bb.lines[:0]
	bb.totalSize = 0
	
	return result
}

func (bb *BatchBuffer) Size() int {
	bb.mu.Lock()
	defer bb.mu.Unlock()
	return len(bb.lines)
}

// NewWavefrontSynthesizer creates a simplified synthesizer
func NewWavefrontSynthesizer(recipe *Recipe) *WavefrontSynthesizer {
	return &WavefrontSynthesizer{
		recipe:     recipe,
		rng:        rand.New(rand.NewSource(time.Now().UnixNano())),
		metricName: recipe.MetricName,
		sources:    []string{"host-001", "host-002", "host-003", "host-004"},
		tags: map[string][]string{
			"env":     {"prod", "staging", "dev"},
			"region":  {"us-east-1", "us-west-2", "eu-west-1"},
			"service": {"web", "api", "db", "cache"},
		},
	}
}

// SynthesizeLine generates a single Wavefront metric line
func (ws *WavefrontSynthesizer) SynthesizeLine(currentTime time.Time, multiplier float64) (string, error) {
	// Generate random metric value
	value := ws.rng.NormFloat64()*50 + 100 // Normal distribution around 100
	if value < 0 {
		value = math.Abs(value)
	}
	value *= multiplier

	// Select random source and tags
	source := ws.sources[ws.rng.Intn(len(ws.sources))]
	
	var tagStrings []string
	for key, values := range ws.tags {
		if ws.rng.Float64() < 0.8 { // 80% chance to include each tag
			tagValue := values[ws.rng.Intn(len(values))]
			tagStrings = append(tagStrings, fmt.Sprintf("%s=%s", key, tagValue))
		}
	}

	// Format: <metric> <value> <timestamp> source=<source> [tags...]
	timestamp := currentTime.Unix()
	line := fmt.Sprintf("%s %.2f %d source=%s", ws.metricName, value, timestamp, source)
	
	for _, tag := range tagStrings {
		line += " " + tag
	}

	return line, nil
}

func NewLoadWorker(config *WorkerConfig) (*LoadWorker, error) {
	// Initialize HTTP clients with connection pooling
	clients := make([]*http.Client, 10) // Pool of 10 clients
	for i := range clients {
		clients[i] = &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 10,
				IdleConnTimeout:     90 * time.Second,
			},
		}
	}

	return &LoadWorker{
		config:       config,
		synthesizers: make(map[string]*WavefrontSynthesizer),
		httpClients:  clients,
		batchBuffer:  NewBatchBuffer(config.BatchSize, 1024*1024), // 1MB buffer
		stopChan:     make(chan struct{}),
	}, nil
}

func (lw *LoadWorker) Start(ctx context.Context) error {
	log.Printf("Starting load worker %s", lw.config.WorkerID)

	// Start metrics server
	go lw.startMetricsServer()

	// Start HTTP server for health checks
	go lw.startHTTPServer()

	// Start assignment poller
	lw.wg.Add(1)
	go lw.assignmentPoller(ctx)

	// Start batch flusher
	lw.wg.Add(1)
	go lw.batchFlusher(ctx)

	// Start traffic generators (will be started when assignments come in)
	
	// Wait for shutdown signal
	<-ctx.Done()
	log.Println("Shutting down load worker...")
	
	close(lw.stopChan)
	lw.wg.Wait()
	
	log.Println("Load worker stopped")
	return nil
}

func (lw *LoadWorker) startMetricsServer() {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		// Simple metrics output
		metricsLock.RLock()
		defer metricsLock.RUnlock()
		
		w.Header().Set("Content-Type", "text/plain")
		for key, value := range linesEmittedCount {
			fmt.Fprintf(w, "loadgen_lines_emitted_total{family_id=\"%s\"} %d\n", key, value)
		}
		for key, value := range bytesEmittedCount {
			fmt.Fprintf(w, "loadgen_bytes_emitted_total{family_id=\"%s\"} %d\n", key, value)
		}
		for key, value := range httpErrorCount {
			fmt.Fprintf(w, "loadgen_http_errors_total{endpoint=\"%s\"} %d\n", key, value)
		}
	})

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", lw.config.MetricsPort),
		Handler: mux,
	}

	log.Printf("Metrics server listening on port %d", lw.config.MetricsPort)
	if err := server.ListenAndServe(); err != nil {
		log.Printf("Metrics server error: %v", err)
	}
}

func (lw *LoadWorker) startHTTPServer() {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", lw.handleHealth)
	mux.HandleFunc("/ready", lw.handleReady)
	mux.HandleFunc("/status", lw.handleStatus)

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", lw.config.Port),
		Handler: mux,
	}

	log.Printf("HTTP server listening on port %d", lw.config.Port)
	if err := server.ListenAndServe(); err != nil {
		log.Printf("HTTP server error: %v", err)
	}
}

func (lw *LoadWorker) handleHealth(w http.ResponseWriter, r *http.Request) {
	lw.mu.RLock()
	hasAssignment := lw.assignment != nil
	synthesizerCount := len(lw.synthesizers)
	lw.mu.RUnlock()

	if hasAssignment && synthesizerCount > 0 {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("No assignment or synthesizers"))
	}
}

func (lw *LoadWorker) handleReady(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("READY"))
}

func (lw *LoadWorker) handleStatus(w http.ResponseWriter, r *http.Request) {
	lw.mu.RLock()
	status := map[string]interface{}{
		"worker_id":     lw.config.WorkerID,
		"has_assignment": lw.assignment != nil,
		"synthesizers":  len(lw.synthesizers),
		"buffer_size":   lw.batchBuffer.Size(),
		"timestamp":     time.Now().UTC(),
	}
	
	if lw.assignment != nil {
		status["assignment"] = lw.assignment
	}
	lw.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

func (lw *LoadWorker) assignmentPoller(ctx context.Context) {
	defer lw.wg.Done()

	ticker := time.NewTicker(lw.config.PollInterval)
	defer ticker.Stop()

	// Initial poll
	lw.pollAssignment(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			lw.pollAssignment(ctx)
		}
	}
}

func (lw *LoadWorker) pollAssignment(ctx context.Context) {
	url := fmt.Sprintf("%s/api/v1/workers/%s/assignment", lw.config.ControlPlaneURL, lw.config.WorkerID)
	
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		log.Printf("Failed to poll assignment: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		// No assignment yet
		return
	}

	if resp.StatusCode != http.StatusOK {
		log.Printf("Assignment poll returned status %d", resp.StatusCode)
		return
	}

	var assignment Assignment
	if err := json.NewDecoder(resp.Body).Decode(&assignment); err != nil {
		log.Printf("Failed to decode assignment: %v", err)
		return
	}

	lw.updateAssignment(&assignment)
}

func (lw *LoadWorker) updateAssignment(assignment *Assignment) {
	lw.mu.Lock()
	defer lw.mu.Unlock()

	// Check if assignment changed
	if lw.assignment != nil && lw.assignmentEqual(lw.assignment, assignment) {
		return // No change
	}

	log.Printf("Updating assignment: %d families, multiplier=%.2f", len(assignment.Families), assignment.Multiplier)

	lw.assignment = assignment

	// Update synthesizers
	lw.updateSynthesizers()

	// Start/restart traffic generators
	lw.restartTrafficGenerators()
}

func (lw *LoadWorker) assignmentEqual(a, b *Assignment) bool {
	if len(a.Families) != len(b.Families) {
		return false
	}
	for i, family := range a.Families {
		if b.Families[i] != family {
			return false
		}
	}
	return a.Multiplier == b.Multiplier && a.BurstFactor == b.BurstFactor
}

func (lw *LoadWorker) updateSynthesizers() {
	// Load recipes for assigned families
	for _, familyID := range lw.assignment.Families {
		if _, exists := lw.synthesizers[familyID]; exists {
			continue // Already have this synthesizer
		}

		recipe, err := lw.loadRecipe(familyID)
		if err != nil {
			log.Printf("Failed to load recipe for family %s: %v", familyID, err)
			continue
		}

		synthesizer := NewWavefrontSynthesizer(recipe)
		lw.synthesizers[familyID] = synthesizer
		log.Printf("Loaded synthesizer for family %s (%s)", familyID[:8], recipe.MetricName)
	}

	// Remove synthesizers for families no longer assigned
	currentFamilies := make(map[string]bool)
	for _, familyID := range lw.assignment.Families {
		currentFamilies[familyID] = true
	}

	for familyID := range lw.synthesizers {
		if !currentFamilies[familyID] {
			delete(lw.synthesizers, familyID)
			log.Printf("Removed synthesizer for family %s", familyID[:8])
		}
	}
}

func (lw *LoadWorker) loadRecipe(familyID string) (*Recipe, error) {
	url := fmt.Sprintf("%s/api/v1/recipes/%s", lw.config.ControlPlaneURL, familyID)
	
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to load recipe: status %d", resp.StatusCode)
	}

	var recipe Recipe
	if err := json.NewDecoder(resp.Body).Decode(&recipe); err != nil {
		return nil, err
	}

	return &recipe, nil
}

func (lw *LoadWorker) restartTrafficGenerators() {
	// Stop existing generators
	close(lw.stopChan)
	lw.stopChan = make(chan struct{})

	// Start new generators for each family
	for familyID, synthesizer := range lw.synthesizers {
		lw.wg.Add(1)
		go lw.trafficGenerator(familyID, synthesizer)
	}
}

func (lw *LoadWorker) trafficGenerator(familyID string, synthesizer *emitters.WavefrontSynthesizer) {
	defer lw.wg.Done()

	log.Printf("Starting traffic generator for family %s", familyID[:8])

	ticker := time.NewTicker(100 * time.Millisecond) // 10 Hz base rate
	defer ticker.Stop()

	lastEmissionTime := time.Now()
	linesEmittedCounter := 0

	for {
		select {
		case <-lw.stopChan:
			log.Printf("Stopping traffic generator for family %s", familyID[:8])
			return
		case now := <-ticker.C:
			lw.mu.RLock()
			assignment := lw.assignment
			lw.mu.RUnlock()

			if assignment == nil {
				continue
			}

			// Calculate target rate based on intensity curve and multiplier
			baseRate := 1.0 // 1 line per second base rate
			targetRate := synthesizer.CalculateTargetRate(now, baseRate, assignment.Multiplier, assignment.BurstFactor)

			// Determine if we should emit in this tick
			timeSinceLastEmission := now.Sub(lastEmissionTime).Seconds()
			expectedLines := targetRate * timeSinceLastEmission
			
			// Emit lines based on expected count (with some randomness)
			linesToEmit := int(expectedLines)
			if expectedLines-float64(linesToEmit) > rand.Float64() {
				linesToEmit++ // Probabilistic rounding
			}

			for i := 0; i < linesToEmit; i++ {
				line, err := synthesizer.SynthesizeLine(now, assignment.Multiplier)
				if err != nil {
					log.Printf("Failed to synthesize line: %v", err)
					continue
				}

				// Add to batch buffer
				if !lw.batchBuffer.Add(line) {
					// Buffer full, force flush
					lw.flushBatch()
					lw.batchBuffer.Add(line) // Retry after flush
				}

				linesEmittedCounter++
				// Update simple metrics
				metricsLock.Lock()
				linesEmittedCount[familyID]++
				bytesEmittedCount[familyID] += int64(len(line))
				metricsLock.Unlock()
			}

			if linesToEmit > 0 {
				lastEmissionTime = now
				
				// Log rate every few seconds
				if linesEmittedCounter%1000 == 0 {
					currentRate := float64(linesEmittedCounter) / time.Since(lastEmissionTime).Seconds()
					log.Printf("Family %s: emitted %d lines at %.1f lines/sec", familyID[:8], linesEmittedCounter, currentRate)
				}
			}
		}
	}
}

func (lw *LoadWorker) batchFlusher(ctx context.Context) {
	defer lw.wg.Done()

	ticker := time.NewTicker(lw.config.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Final flush before shutdown
			lw.flushBatch()
			return
		case <-ticker.C:
			lw.flushBatch()
		}
	}
}

func (lw *LoadWorker) flushBatch() {
	lines := lw.batchBuffer.Flush()
	if len(lines) == 0 {
		return
	}

	// Get endpoints from assignment
	lw.mu.RLock()
	assignment := lw.assignment
	lw.mu.RUnlock()

	if assignment == nil {
		return
	}

	// Construct batch payload
	var payload bytes.Buffer
	for _, line := range lines {
		payload.WriteString(line)
		payload.WriteString("\n")
	}

	// Send to endpoints (simplified - would use old loadgen auth)
	endpoints := []string{"http://collectors:8080/api/v2/wfproxy/report"} // Default endpoint
	
	for _, endpoint := range endpoints {
		if err := lw.sendBatch(endpoint, payload.Bytes()); err != nil {
			log.Printf("Failed to send batch to %s: %v", endpoint, err)
			// Update error metrics
			metricsLock.Lock()
			httpErrorCount[endpoint]++
			metricsLock.Unlock()
		}
	}

	log.Printf("Flushed batch of %d lines (%d bytes)", len(lines), payload.Len())
}

func (lw *LoadWorker) sendBatch(endpoint string, payload []byte) error {
	// Get HTTP client from pool
	clientIdx := int(time.Now().UnixNano()) % len(lw.httpClients)
	client := lw.httpClients[clientIdx]

	// Create request
	req, err := http.NewRequest("POST", endpoint, bytes.NewReader(payload))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("User-Agent", "loadgen-worker/1.0")

	// Simple authentication - could be enhanced
	// req.Header.Set("Authorization", "Bearer token-here")

	// Send request
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Check response
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		metricsLock.Lock()
		httpErrorCount[endpoint+":"+strconv.Itoa(resp.StatusCode)]++
		metricsLock.Unlock()
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

func getWorkerID() string {
	// Try to get from environment
	if id := os.Getenv("WORKER_ID"); id != "" {
		return id
	}

	// Try to get pod name from Kubernetes
	if podName := os.Getenv("HOSTNAME"); podName != "" {
		return podName
	}

	// Generate random ID
	return fmt.Sprintf("worker-%d", time.Now().UnixNano()%10000)
}

func main() {
	var (
		workerID        = flag.String("worker-id", getWorkerID(), "Worker ID")
		controlPlaneURL = flag.String("control-plane-url", defaultControlPlaneURL, "Control plane URL")
		port            = flag.Int("port", defaultPort, "HTTP port")
		metricsPort     = flag.Int("metrics-port", defaultMetricsPort, "Metrics port")
		pollInterval    = flag.Duration("poll-interval", defaultPollInterval, "Assignment poll interval")
		batchSize       = flag.Int("batch-size", defaultBatchSize, "Batch size for emission")
		flushInterval   = flag.Duration("flush-interval", defaultFlushInterval, "Batch flush interval")
	)
	flag.Parse()

	config := &WorkerConfig{
		WorkerID:         *workerID,
		ControlPlaneURL:  *controlPlaneURL,
		Port:            *port,
		MetricsPort:     *metricsPort,
		PollInterval:    *pollInterval,
		BatchSize:       *batchSize,
		FlushInterval:   *flushInterval,
	}

	worker, err := NewLoadWorker(config)
	if err != nil {
		log.Fatalf("Failed to create worker: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		log.Println("Shutting down worker...")
		cancel()
	}()

	if err := worker.Start(ctx); err != nil {
		log.Fatalf("Worker failed: %v", err)
	}
}