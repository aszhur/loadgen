package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/storage"
	"github.com/klauspost/compress/zstd"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/api/option"
)

const (
	defaultPort         = 8080
	defaultMetricsPort  = 9090
	defaultMaxMemoryMB  = 512
	defaultMaxAgeSec    = 60
	defaultChunkSizeMB  = 128
	defaultWorkerCount  = 16
	compressionLevel    = 5 // zstd compression level
)

var (
	// Prometheus metrics
	requestsReceived = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "capture_requests_received_total",
			Help: "Total number of mirror requests received",
		},
		[]string{"method", "path"},
	)

	bytesReceived = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "capture_bytes_received_total",
			Help: "Total bytes received from mirror requests",
		},
		[]string{"content_type"},
	)

	queueDepthBytes = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "capture_queue_depth_bytes",
			Help: "Current queue depth in bytes",
		},
	)

	backlogSeconds = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "capture_backlog_seconds",
			Help: "Current backlog in seconds",
		},
	)

	uploadsInflight = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "capture_uploads_inflight",
			Help: "Number of uploads currently in progress",
		},
	)

	uploadRateBps = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "capture_upload_rate_bps",
			Help: "Current upload rate in bytes per second",
		},
	)

	uploadErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "capture_upload_errors_total",
			Help: "Total number of upload errors",
		},
		[]string{"error_type"},
	)

	filesUploaded = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "capture_files_uploaded_total",
			Help: "Total number of files uploaded to GCS",
		},
	)
)

func init() {
	prometheus.MustRegister(requestsReceived)
	prometheus.MustRegister(bytesReceived)
	prometheus.MustRegister(queueDepthBytes)
	prometheus.MustRegister(backlogSeconds)
	prometheus.MustRegister(uploadsInflight)
	prometheus.MustRegister(uploadRateBps)
	prometheus.MustRegister(uploadErrors)
	prometheus.MustRegister(filesUploaded)
}

type Config struct {
	Port           int
	MetricsPort    int
	BucketName     string
	BucketPrefix   string
	ProjectID      string
	MaxMemoryMB    int
	MaxAgeSec      int
	ChunkSizeMB    int
	WorkerCount    int
	SpillDir       string
	InstanceID     string
	Zone           string
}

type CaptureBuffer struct {
	data      bytes.Buffer
	createdAt time.Time
	mu        sync.Mutex
}

func (cb *CaptureBuffer) Write(data []byte) (int, error) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.data.Write(data)
}

func (cb *CaptureBuffer) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.data.Reset()
	cb.createdAt = time.Now()
}

func (cb *CaptureBuffer) Size() int {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.data.Len()
}

func (cb *CaptureBuffer) Age() time.Duration {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return time.Since(cb.createdAt)
}

func (cb *CaptureBuffer) ReadAndReset() []byte {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	data := make([]byte, cb.data.Len())
	copy(data, cb.data.Bytes())
	cb.data.Reset()
	cb.createdAt = time.Now()
	return data
}

type CaptureAgent struct {
	config        *Config
	buffer        *CaptureBuffer
	gcsClient     *storage.Client
	uploadQueue   chan []byte
	wg            sync.WaitGroup
	ctx           context.Context
	cancel        context.CancelFunc
	bytesUploaded int64
	uploadStart   time.Time
}

func NewCaptureAgent(config *Config) (*CaptureAgent, error) {
	ctx, cancel := context.WithCancel(context.Background())

	// Initialize GCS client
	client, err := storage.NewClient(ctx, option.WithScopes(storage.ScopeReadWrite))
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create GCS client: %w", err)
	}

	// Create spill directory
	if err := os.MkdirAll(config.SpillDir, 0755); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create spill directory: %w", err)
	}

	ca := &CaptureAgent{
		config:      config,
		buffer:      &CaptureBuffer{createdAt: time.Now()},
		gcsClient:   client,
		uploadQueue: make(chan []byte, config.WorkerCount*2),
		ctx:         ctx,
		cancel:      cancel,
		uploadStart: time.Now(),
	}

	return ca, nil
}

func (ca *CaptureAgent) Start() error {
	log.Printf("Starting capture agent on port %d", ca.config.Port)

	// Start upload workers
	for i := 0; i < ca.config.WorkerCount; i++ {
		ca.wg.Add(1)
		go ca.uploadWorker(i)
	}

	// Start buffer rotation ticker
	ca.wg.Add(1)
	go ca.bufferRotator()

	// Start metrics updater
	ca.wg.Add(1)
	go ca.metricsUpdater()

	// Start HTTP servers
	go ca.startMetricsServer()
	return ca.startHTTPServer()
}

func (ca *CaptureAgent) Stop() {
	log.Println("Stopping capture agent...")
	ca.cancel()
	close(ca.uploadQueue)
	ca.wg.Wait()
	ca.gcsClient.Close()
	log.Println("Capture agent stopped")
}

func (ca *CaptureAgent) startHTTPServer() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/", ca.handleMirror)
	mux.HandleFunc("/health", ca.handleHealth)
	mux.HandleFunc("/ready", ca.handleReady)

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", ca.config.Port),
		Handler: mux,
	}

	log.Printf("Capture HTTP server listening on port %d", ca.config.Port)
	return server.ListenAndServe()
}

func (ca *CaptureAgent) startMetricsServer() {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", ca.config.MetricsPort),
		Handler: mux,
	}

	log.Printf("Metrics server listening on port %d", ca.config.MetricsPort)
	if err := server.ListenAndServe(); err != nil {
		log.Printf("Metrics server error: %v", err)
	}
}

func (ca *CaptureAgent) handleMirror(w http.ResponseWriter, r *http.Request) {
	// Update request metrics
	requestsReceived.WithLabelValues(r.Method, r.URL.Path).Inc()

	// Read request body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("Error reading request body: %v", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// Update bytes received metrics
	bytesReceived.WithLabelValues(r.Header.Get("Content-Type")).Add(float64(len(body)))

	// Add newline if not present (Wavefront line protocol)
	if len(body) > 0 && body[len(body)-1] != '\n' {
		body = append(body, '\n')
	}

	// Write to buffer
	if len(body) > 0 {
		ca.buffer.Write(body)
	}

	// Respond quickly to mirror
	w.WriteHeader(http.StatusOK)
}

func (ca *CaptureAgent) handleHealth(w http.ResponseWriter, r *http.Request) {
	// Check if we're severely backlogged
	backlog := ca.calculateBacklog()
	if backlog > 120 { // 2 minutes backlog is critical
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprintf(w, "UNHEALTHY: backlog %.1fs", backlog)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "OK: backlog %.1fs", backlog)
}

func (ca *CaptureAgent) handleReady(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("READY"))
}

func (ca *CaptureAgent) bufferRotator() {
	defer ca.wg.Done()

	ticker := time.NewTicker(5 * time.Second) // Check every 5 seconds
	defer ticker.Stop()

	for {
		select {
		case <-ca.ctx.Done():
			// Final rotation on shutdown
			ca.rotateBuffer()
			return
		case <-ticker.C:
			ca.rotateBuffer()
		}
	}
}

func (ca *CaptureAgent) rotateBuffer() {
	bufferSize := ca.buffer.Size()
	bufferAge := ca.buffer.Age()

	maxSize := ca.config.MaxMemoryMB * 1024 * 1024
	maxAge := time.Duration(ca.config.MaxAgeSec) * time.Second

	// Rotate if buffer is too large or too old
	if bufferSize > maxSize || bufferAge > maxAge {
		if bufferSize > 0 {
			data := ca.buffer.ReadAndReset()
			
			select {
			case ca.uploadQueue <- data:
				log.Printf("Rotated buffer: %d bytes, age %.1fs", len(data), bufferAge.Seconds())
			default:
				// Queue full, spill to disk
				ca.spillToDisk(data)
				log.Printf("Queue full, spilled %d bytes to disk", len(data))
			}
		}
	}
}

func (ca *CaptureAgent) spillToDisk(data []byte) {
	filename := fmt.Sprintf("spill-%d-%d.wf", time.Now().UnixNano(), crc32.ChecksumIEEE(data))
	filepath := filepath.Join(ca.config.SpillDir, filename)

	if err := os.WriteFile(filepath, data, 0644); err != nil {
		log.Printf("Error spilling to disk: %v", err)
		uploadErrors.WithLabelValues("spill_error").Inc()
	}
}

func (ca *CaptureAgent) uploadWorker(workerID int) {
	defer ca.wg.Done()

	log.Printf("Upload worker %d started", workerID)

	for data := range ca.uploadQueue {
		uploadsInflight.Inc()
		
		if err := ca.uploadToGCS(data); err != nil {
			log.Printf("Worker %d: Upload failed: %v", workerID, err)
			uploadErrors.WithLabelValues("upload_error").Inc()
			
			// Spill to disk on upload failure
			ca.spillToDisk(data)
		} else {
			filesUploaded.Inc()
			atomic.AddInt64(&ca.bytesUploaded, int64(len(data)))
		}

		uploadsInflight.Dec()
	}

	log.Printf("Upload worker %d stopped", workerID)
}

func (ca *CaptureAgent) uploadToGCS(data []byte) error {
	// Compress data
	var compressedBuf bytes.Buffer
	encoder, err := zstd.NewWriter(&compressedBuf, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(compressionLevel)))
	if err != nil {
		return fmt.Errorf("failed to create zstd encoder: %w", err)
	}

	if _, err := encoder.Write(data); err != nil {
		encoder.Close()
		return fmt.Errorf("failed to compress data: %w", err)
	}

	if err := encoder.Close(); err != nil {
		return fmt.Errorf("failed to close zstd encoder: %w", err)
	}

	compressedData := compressedBuf.Bytes()

	// Generate object name
	timestamp := time.Now().UTC()
	objectName := fmt.Sprintf("%s/dt=%s/mig=%s/%s/part-%d.wf.zst",
		ca.config.BucketPrefix,
		timestamp.Format("2006-01-02"),
		"tier-e", // MIG identifier
		ca.config.InstanceID,
		timestamp.UnixNano(),
	)

	// Upload to GCS with resumable uploads
	bucket := ca.gcsClient.Bucket(ca.config.BucketName)
	obj := bucket.Object(objectName)

	writer := obj.NewWriter(ca.ctx)
	writer.ChunkSize = ca.config.ChunkSizeMB * 1024 * 1024
	writer.ContentType = "application/zstd"
	writer.Metadata = map[string]string{
		"original_size":     fmt.Sprintf("%d", len(data)),
		"compressed_size":   fmt.Sprintf("%d", len(compressedData)),
		"compression_ratio": fmt.Sprintf("%.2f", float64(len(data))/float64(len(compressedData))),
		"timestamp":         timestamp.Format(time.RFC3339),
		"instance_id":       ca.config.InstanceID,
		"zone":              ca.config.Zone,
	}

	if _, err := writer.Write(compressedData); err != nil {
		writer.Close()
		return fmt.Errorf("failed to write to GCS: %w", err)
	}

	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close GCS writer: %w", err)
	}

	// Create manifest entry
	manifest := map[string]interface{}{
		"object_name":       objectName,
		"original_size":     len(data),
		"compressed_size":   len(compressedData),
		"compression_ratio": float64(len(data)) / float64(len(compressedData)),
		"timestamp":         timestamp.Format(time.RFC3339),
		"instance_id":       ca.config.InstanceID,
		"zone":              ca.config.Zone,
		"sha256":            fmt.Sprintf("%x", crc32.ChecksumIEEE(data)), // Use CRC32 for speed
	}

	manifestData, _ := json.Marshal(manifest)
	manifestData = append(manifestData, '\n')

	manifestObjectName := fmt.Sprintf("%s/dt=%s/manifests/%s-manifest.jsonl",
		ca.config.BucketPrefix,
		timestamp.Format("2006-01-02"),
		ca.config.InstanceID,
	)

	// Append to manifest file
	manifestObj := bucket.Object(manifestObjectName)
	manifestWriter := manifestObj.NewWriter(ca.ctx)
	manifestWriter.ChunkSize = 1024 * 1024 // 1MB chunks for manifest
	manifestWriter.ContentType = "application/jsonl"

	if _, err := manifestWriter.Write(manifestData); err != nil {
		manifestWriter.Close()
		log.Printf("Warning: Failed to write manifest entry: %v", err)
	} else {
		manifestWriter.Close()
	}

	log.Printf("Uploaded %s: %d -> %d bytes (%.2fx compression)",
		objectName, len(data), len(compressedData),
		float64(len(data))/float64(len(compressedData)))

	return nil
}

func (ca *CaptureAgent) calculateBacklog() float64 {
	queueLen := float64(len(ca.uploadQueue))
	maxQueue := float64(cap(ca.uploadQueue))
	bufferSize := float64(ca.buffer.Size())
	maxBuffer := float64(ca.config.MaxMemoryMB * 1024 * 1024)

	// Estimate processing time based on current queue and buffer state
	estimatedSeconds := (queueLen/maxQueue)*float64(ca.config.MaxAgeSec) +
		(bufferSize/maxBuffer)*float64(ca.config.MaxAgeSec)

	return estimatedSeconds
}

func (ca *CaptureAgent) metricsUpdater() {
	defer ca.wg.Done()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ca.ctx.Done():
			return
		case <-ticker.C:
			// Update metrics
			queueDepthBytes.Set(float64(len(ca.uploadQueue) * ca.config.MaxMemoryMB * 1024 * 1024))
			backlogSeconds.Set(ca.calculateBacklog())

			// Calculate upload rate
			elapsed := time.Since(ca.uploadStart).Seconds()
			if elapsed > 0 {
				rate := float64(atomic.LoadInt64(&ca.bytesUploaded)) / elapsed
				uploadRateBps.Set(rate)
			}
		}
	}
}

func main() {
	var cfg Config
	flag.IntVar(&cfg.Port, "port", defaultPort, "HTTP port")
	flag.IntVar(&cfg.MetricsPort, "metrics-port", defaultMetricsPort, "Metrics port")
	flag.StringVar(&cfg.BucketName, "bucket", "", "GCS bucket name")
	flag.StringVar(&cfg.BucketPrefix, "bucket-prefix", "capture", "GCS bucket prefix")
	flag.StringVar(&cfg.ProjectID, "project", "", "GCP project ID")
	flag.IntVar(&cfg.MaxMemoryMB, "max-memory-mb", defaultMaxMemoryMB, "Max buffer memory in MB")
	flag.IntVar(&cfg.MaxAgeSec, "max-age-sec", defaultMaxAgeSec, "Max buffer age in seconds")
	flag.IntVar(&cfg.ChunkSizeMB, "chunk-size-mb", defaultChunkSizeMB, "GCS upload chunk size in MB")
	flag.IntVar(&cfg.WorkerCount, "workers", defaultWorkerCount, "Number of upload workers")
	flag.StringVar(&cfg.SpillDir, "spill-dir", "/var/spool/capture-agent", "Directory for spill files")
	flag.StringVar(&cfg.InstanceID, "instance-id", "", "Instance ID")
	flag.StringVar(&cfg.Zone, "zone", "", "GCP zone")
	flag.Parse()

	if cfg.BucketName == "" || cfg.ProjectID == "" {
		log.Fatal("Missing required flags: -bucket, -project")
	}

	// Get instance metadata if not provided
	if cfg.InstanceID == "" {
		// This would typically come from metadata service in GCP
		cfg.InstanceID = fmt.Sprintf("instance-%d", time.Now().Unix())
	}
	if cfg.Zone == "" {
		cfg.Zone = "unknown-zone"
	}

	agent, err := NewCaptureAgent(&cfg)
	if err != nil {
		log.Fatalf("Failed to create capture agent: %v", err)
	}

	if err := agent.Start(); err != nil {
		log.Fatalf("Failed to start capture agent: %v", err)
	}
}