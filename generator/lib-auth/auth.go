package libauth

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// AuthManager handles authentication and connection management for Wavefront endpoints
type AuthManager struct {
	connections map[string]*ConnectionPool
	mu          sync.RWMutex
}

// ConnectionPool manages a pool of connections to a specific endpoint
type ConnectionPool struct {
	endpoint string
	auth     AuthConfig
	conns    chan net.Conn
	mu       sync.Mutex
	maxConns int
}

// AuthConfig holds authentication configuration
type AuthConfig struct {
	Type   string            `json:"type" yaml:"type"`
	Token  string            `json:"token,omitempty" yaml:"token,omitempty"`
	Headers map[string]string `json:"headers,omitempty" yaml:"headers,omitempty"`
}

// NewAuthManager creates a new authentication manager
func NewAuthManager() (*AuthManager, error) {
	return &AuthManager{
		connections: make(map[string]*ConnectionPool),
	}, nil
}

// ApplyAuth applies authentication to an HTTP request
func (am *AuthManager) ApplyAuth(req *http.Request) error {
	// For now, this is a simple implementation
	// In a real implementation, you might add Bearer tokens, API keys, etc.
	
	// Add common headers
	req.Header.Set("User-Agent", "wavefront-loadgen/2.0")
	req.Header.Set("Content-Type", "text/plain")
	
	// You could extend this to support different auth types:
	// - Bearer tokens
	// - API keys
	// - Custom authentication schemes
	
	return nil
}

// GetConnection gets a connection from the pool or creates a new one
func (am *AuthManager) GetConnection(endpoint string) (net.Conn, error) {
	am.mu.RLock()
	pool, exists := am.connections[endpoint]
	am.mu.RUnlock()
	
	if !exists {
		am.mu.Lock()
		// Check again after acquiring write lock
		if pool, exists = am.connections[endpoint]; !exists {
			pool = &ConnectionPool{
				endpoint: endpoint,
				conns:    make(chan net.Conn, 10),
				maxConns: 10,
			}
			am.connections[endpoint] = pool
		}
		am.mu.Unlock()
	}
	
	return pool.Get()
}

// ReturnConnection returns a connection to the pool
func (am *AuthManager) ReturnConnection(endpoint string, conn net.Conn) {
	am.mu.RLock()
	pool, exists := am.connections[endpoint]
	am.mu.RUnlock()
	
	if exists {
		pool.Return(conn)
	} else {
		conn.Close()
	}
}

// Get retrieves a connection from the pool
func (cp *ConnectionPool) Get() (net.Conn, error) {
	select {
	case conn := <-cp.conns:
		// Test if connection is still valid
		conn.SetDeadline(time.Now().Add(1 * time.Millisecond))
		_, err := conn.Write([]byte{})
		conn.SetDeadline(time.Time{}) // Reset deadline
		
		if err != nil {
			conn.Close()
			return cp.createConnection()
		}
		return conn, nil
	default:
		return cp.createConnection()
	}
}

// Return returns a connection to the pool
func (cp *ConnectionPool) Return(conn net.Conn) {
	if conn == nil {
		return
	}
	
	select {
	case cp.conns <- conn:
		// Successfully returned to pool
	default:
		// Pool is full, close the connection
		conn.Close()
	}
}

func (cp *ConnectionPool) createConnection() (net.Conn, error) {
	// Parse endpoint to get host and port
	// For now, assume endpoint format like "host:port"
	conn, err := net.DialTimeout("tcp", cp.endpoint, 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", cp.endpoint, err)
	}
	
	// Set connection options
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetKeepAlive(true)
		tcpConn.SetKeepAlivePeriod(30 * time.Second)
		tcpConn.SetNoDelay(true)
	}
	
	return conn, nil
}

// BufferedWriter wraps a connection with buffering similar to Java's BufferedOutputStream
type BufferedWriter struct {
	conn   net.Conn
	writer *bufio.Writer
	mu     sync.Mutex
}

// NewBufferedWriter creates a new buffered writer
func NewBufferedWriter(conn net.Conn, bufferSize int) *BufferedWriter {
	if bufferSize <= 0 {
		bufferSize = 8192 // Default 8KB buffer like Java version
	}
	
	return &BufferedWriter{
		conn:   conn,
		writer: bufio.NewWriterSize(conn, bufferSize),
	}
}

// Write writes data to the buffered writer
func (bw *BufferedWriter) Write(data []byte) (int, error) {
	bw.mu.Lock()
	defer bw.mu.Unlock()
	return bw.writer.Write(data)
}

// WriteString writes a string to the buffered writer
func (bw *BufferedWriter) WriteString(s string) (int, error) {
	bw.mu.Lock()
	defer bw.mu.Unlock()
	return bw.writer.WriteString(s)
}

// Flush flushes the buffer
func (bw *BufferedWriter) Flush() error {
	bw.mu.Lock()
	defer bw.mu.Unlock()
	return bw.writer.Flush()
}

// Close closes the writer and underlying connection
func (bw *BufferedWriter) Close() error {
	bw.mu.Lock()
	defer bw.mu.Unlock()
	
	if err := bw.writer.Flush(); err != nil {
		bw.conn.Close()
		return err
	}
	
	return bw.conn.Close()
}

// WavefrontClient provides a high-level client for sending Wavefront data
type WavefrontClient struct {
	authManager *AuthManager
	endpoint    string
	bufferSize  int
	flushPeriod time.Duration
	writer      *BufferedWriter
	mu          sync.Mutex
}

// NewWavefrontClient creates a new Wavefront client
func NewWavefrontClient(endpoint string, bufferSize int, flushPeriod time.Duration) (*WavefrontClient, error) {
	authManager, err := NewAuthManager()
	if err != nil {
		return nil, err
	}
	
	client := &WavefrontClient{
		authManager: authManager,
		endpoint:    endpoint,
		bufferSize:  bufferSize,
		flushPeriod: flushPeriod,
	}
	
	if err := client.connect(); err != nil {
		return nil, err
	}
	
	// Start periodic flushing if configured
	if flushPeriod > 0 {
		go client.periodicFlush()
	}
	
	return client, nil
}

func (wc *WavefrontClient) connect() error {
	wc.mu.Lock()
	defer wc.mu.Unlock()
	
	conn, err := wc.authManager.GetConnection(wc.endpoint)
	if err != nil {
		return err
	}
	
	wc.writer = NewBufferedWriter(conn, wc.bufferSize)
	return nil
}

func (wc *WavefrontClient) periodicFlush() {
	ticker := time.NewTicker(wc.flushPeriod)
	defer ticker.Stop()
	
	for range ticker.C {
		wc.mu.Lock()
		if wc.writer != nil {
			wc.writer.Flush()
		}
		wc.mu.Unlock()
	}
}

// SendLine sends a single Wavefront line
func (wc *WavefrontClient) SendLine(line string) error {
	wc.mu.Lock()
	defer wc.mu.Unlock()
	
	if wc.writer == nil {
		if err := wc.connect(); err != nil {
			return err
		}
	}
	
	_, err := wc.writer.WriteString(line + "\n")
	return err
}

// SendBatch sends multiple lines in a batch
func (wc *WavefrontClient) SendBatch(lines []string) error {
	wc.mu.Lock()
	defer wc.mu.Unlock()
	
	if wc.writer == nil {
		if err := wc.connect(); err != nil {
			return err
		}
	}
	
	for _, line := range lines {
		if _, err := wc.writer.WriteString(line + "\n"); err != nil {
			return err
		}
	}
	
	// Flush after batch
	return wc.writer.Flush()
}

// Flush forces a flush of the buffer
func (wc *WavefrontClient) Flush() error {
	wc.mu.Lock()
	defer wc.mu.Unlock()
	
	if wc.writer != nil {
		return wc.writer.Flush()
	}
	return nil
}

// Close closes the client
func (wc *WavefrontClient) Close() error {
	wc.mu.Lock()
	defer wc.mu.Unlock()
	
	if wc.writer != nil {
		return wc.writer.Close()
	}
	return nil
}

// Simple helper for HTTP-based sending (alternative to socket-based)
type HTTPSender struct {
	client   *http.Client
	endpoint string
	auth     AuthConfig
}

// NewHTTPSender creates a new HTTP-based sender
func NewHTTPSender(endpoint string, auth AuthConfig) *HTTPSender {
	return &HTTPSender{
		client: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 10,
				IdleConnTimeout:     90 * time.Second,
			},
		},
		endpoint: endpoint,
		auth:     auth,
	}
}

// SendBatch sends a batch via HTTP POST
func (hs *HTTPSender) SendBatch(lines []string) error {
	payload := ""
	for _, line := range lines {
		payload += line + "\n"
	}
	
	req, err := http.NewRequest("POST", hs.endpoint, strings.NewReader(payload))
	if err != nil {
		return err
	}
	
	// Apply authentication
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("User-Agent", "wavefront-loadgen/2.0")
	
	if hs.auth.Type == "bearer" && hs.auth.Token != "" {
		req.Header.Set("Authorization", "Bearer "+hs.auth.Token)
	}
	
	for k, v := range hs.auth.Headers {
		req.Header.Set(k, v)
	}
	
	resp, err := hs.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}
	
	return nil
}