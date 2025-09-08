package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	core "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	endpoint "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	runtime "github.com/envoyproxy/go-control-plane/envoy/service/runtime/v3"
	"github.com/envoyproxy/go-control-plane/pkg/cache/types"
	"github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	xds "github.com/envoyproxy/go-control-plane/pkg/server/v3"

	compute "google.golang.org/api/compute/v1"
)

const (
	grpcPort           = 18000
	httpPort           = 8080
	xdsClusterName     = "loadgen-xds-controller"
	nodeIDPrefix       = "loadgen-envoy"
	discoveryInterval  = 30 * time.Second
	captureRTDSKey     = "capture.enabled"
)

type Config struct {
	ProjectID        string
	CollectorMIG     string
	CaptureAgentMIG  string
	Zone             string
	Port             int
	LogLevel         string
}

type Controller struct {
	config      *Config
	cache       cache.SnapshotCache
	computeSvc  *compute.Service
	mu          sync.RWMutex
	version     int64
	captureRate float64
}

func main() {
	var cfg Config
	flag.StringVar(&cfg.ProjectID, "project", "", "GCP Project ID")
	flag.StringVar(&cfg.CollectorMIG, "collector-mig", "", "Collector MIG name")
	flag.StringVar(&cfg.CaptureAgentMIG, "capture-mig", "", "Capture Agent MIG name")
	flag.StringVar(&cfg.Zone, "zone", "", "GCP Zone")
	flag.IntVar(&cfg.Port, "port", grpcPort, "gRPC port")
	flag.StringVar(&cfg.LogLevel, "log-level", "info", "Log level")
	flag.Parse()

	if cfg.ProjectID == "" || cfg.CollectorMIG == "" || cfg.CaptureAgentMIG == "" || cfg.Zone == "" {
		log.Fatal("Missing required flags: -project, -collector-mig, -capture-mig, -zone")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize compute service
	computeSvc, err := compute.NewService(ctx)
	if err != nil {
		log.Fatalf("Failed to create compute service: %v", err)
	}

	// Create controller
	controller := &Controller{
		config:      &cfg,
		cache:       cache.NewSnapshotCache(false, cache.IDHash{}, nil),
		computeSvc:  computeSvc,
		captureRate: 0.0, // Start with capture disabled
	}

	// Start discovery loop
	go controller.discoveryLoop(ctx)

	// Start gRPC server
	server := xds.NewServer(ctx, controller.cache, nil)
	grpcServer := grpc.NewServer()
	discovery.RegisterAggregatedDiscoveryServiceServer(grpcServer, server)
	runtime.RegisterRuntimeDiscoveryServiceServer(grpcServer, server)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.Port))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	go func() {
		log.Printf("Starting xDS server on port %d", cfg.Port)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("Failed to serve: %v", err)
		}
	}()

	// Start HTTP management server
	go controller.startHTTPServer()

	// Handle shutdown
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c

	log.Println("Shutting down...")
	grpcServer.GracefulStop()
	cancel()
}

func (c *Controller) discoveryLoop(ctx context.Context) {
	ticker := time.NewTicker(discoveryInterval)
	defer ticker.Stop()

	// Initial discovery
	c.updateSnapshot(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.updateSnapshot(ctx)
		}
	}
}

func (c *Controller) updateSnapshot(ctx context.Context) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.version++
	log.Printf("Updating snapshot version %d", c.version)

	// Discover collector instances
	collectorEndpoints, err := c.discoverEndpoints(ctx, c.config.CollectorMIG)
	if err != nil {
		log.Printf("Failed to discover collector endpoints: %v", err)
		return
	}

	// Discover capture agent instances
	captureEndpoints, err := c.discoverEndpoints(ctx, c.config.CaptureAgentMIG)
	if err != nil {
		log.Printf("Failed to discover capture agent endpoints: %v", err)
		return
	}

	// Create EDS resources
	collectorCluster := c.createClusterLoadAssignment("collector_cluster", collectorEndpoints)
	captureCluster := c.createClusterLoadAssignment("capture_cluster", captureEndpoints)

	// Create RTDS resource
	rtdsRuntime := c.createRuntimeLayer()

	// Create snapshot
	snapshot, err := cache.NewSnapshot(
		fmt.Sprintf("%d", c.version),
		map[string][]types.Resource{
			types.Endpoint: {collectorCluster, captureCluster},
			types.Runtime:  {rtdsRuntime},
		},
	)
	if err != nil {
		log.Printf("Failed to create snapshot: %v", err)
		return
	}

	// Update cache for all Envoy nodes
	nodeHash := cache.IDHash{}
	for _, endpoint := range append(collectorEndpoints, captureEndpoints...) {
		nodeID := fmt.Sprintf("%s-%s", nodeIDPrefix, endpoint.Zone)
		if err := c.cache.SetSnapshot(ctx, nodeHash.ID(&core.Node{Id: nodeID}), snapshot); err != nil {
			log.Printf("Failed to set snapshot for node %s: %v", nodeID, err)
		}
	}

	log.Printf("Updated snapshot: %d collectors, %d capture agents, capture_rate=%.1f%%", 
		len(collectorEndpoints), len(captureEndpoints), c.captureRate*100)
}

type Endpoint struct {
	Address string
	Port    uint32
	Zone    string
	Healthy bool
}

func (c *Controller) discoverEndpoints(ctx context.Context, migName string) ([]Endpoint, error) {
	instances, err := c.computeSvc.InstanceGroupManagers.ListManagedInstances(
		c.config.ProjectID, c.config.Zone, migName).Context(ctx).Do()
	if err != nil {
		return nil, fmt.Errorf("failed to list managed instances: %w", err)
	}

	var endpoints []Endpoint
	for _, instance := range instances.ManagedInstances {
		// Skip instances that are being deleted
		if instance.InstanceStatus == "DELETING" || instance.InstanceStatus == "STOPPING" {
			continue
		}

		// Extract zone and instance name from URL
		parts := parseInstanceURL(instance.Instance)
		if len(parts) < 2 {
			log.Printf("Failed to parse instance URL: %s", instance.Instance)
			continue
		}

		// Get instance details for IP address
		inst, err := c.computeSvc.Instances.Get(c.config.ProjectID, parts[0], parts[1]).Context(ctx).Do()
		if err != nil {
			log.Printf("Failed to get instance details for %s: %v", parts[1], err)
			continue
		}

		if len(inst.NetworkInterfaces) == 0 {
			log.Printf("No network interfaces found for instance %s", parts[1])
			continue
		}

		// Use internal IP
		ip := inst.NetworkInterfaces[0].NetworkIP
		healthy := instance.InstanceStatus == "RUNNING"

		endpoints = append(endpoints, Endpoint{
			Address: ip,
			Port:    8080, // Default service port
			Zone:    parts[0],
			Healthy: healthy,
		})
	}

	return endpoints, nil
}

func (c *Controller) createClusterLoadAssignment(clusterName string, endpoints []Endpoint) *endpoint.ClusterLoadAssignment {
	var lbEndpoints []*endpoint.LbEndpoint
	
	for _, ep := range endpoints {
		weight := uint32(100)
		if !ep.Healthy {
			weight = 0 // Drain unhealthy endpoints
		}

		lbEndpoints = append(lbEndpoints, &endpoint.LbEndpoint{
			HostIdentifier: &endpoint.LbEndpoint_Endpoint{
				Endpoint: &endpoint.Endpoint{
					Address: &core.Address{
						Address: &core.Address_SocketAddress{
							SocketAddress: &core.SocketAddress{
								Protocol: core.SocketAddress_TCP,
								Address:  ep.Address,
								PortSpecifier: &core.SocketAddress_PortValue{
									PortValue: ep.Port,
								},
							},
						},
					},
				},
			},
			LoadBalancingWeight: &wrapperspb.UInt32Value{Value: weight},
		})
	}

	return &endpoint.ClusterLoadAssignment{
		ClusterName: clusterName,
		Endpoints: []*endpoint.LocalityLbEndpoints{
			{
				Locality: &core.Locality{
					Region: "us-central1", // TODO: Make configurable
					Zone:   c.config.Zone,
				},
				LbEndpoints: lbEndpoints,
			},
		},
	}
}

func (c *Controller) createRuntimeLayer() *runtime.Runtime {
	return &runtime.Runtime{
		Name: "loadgen_runtime",
		Layer: &structpb.Struct{
			Fields: map[string]*structpb.Value{
				captureRTDSKey: {
					Kind: &structpb.Value_NumberValue{
						NumberValue: c.captureRate * 100, // Convert to percentage
					},
				},
			},
		},
	}
}

func (c *Controller) startHTTPServer() {
	mux := http.NewServeMux()
	
	// Health endpoint
	mux.HandleFunc("/health", c.handleHealth)
	
	// Runtime control endpoints
	mux.HandleFunc("/capture/enable", c.handleCaptureEnable)
	mux.HandleFunc("/capture/disable", c.handleCaptureDisable)
	mux.HandleFunc("/capture/rate", c.handleCaptureRate)
	mux.HandleFunc("/status", c.handleStatus)

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", httpPort),
		Handler: mux,
	}

	log.Printf("Starting HTTP management server on port %d", httpPort)
	if err := server.ListenAndServe(); err != http.ErrServerClosed {
		log.Printf("HTTP server error: %v", err)
	}
}

func (c *Controller) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func (c *Controller) handleCaptureEnable(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	rate := r.URL.Query().Get("rate")
	if rate == "" {
		rate = "100"
	}

	var newRate float64
	if _, err := fmt.Sscanf(rate, "%f", &newRate); err != nil {
		http.Error(w, "Invalid rate parameter", http.StatusBadRequest)
		return
	}

	if newRate < 0 || newRate > 100 {
		http.Error(w, "Rate must be between 0 and 100", http.StatusBadRequest)
		return
	}

	c.mu.Lock()
	c.captureRate = newRate / 100.0
	c.mu.Unlock()

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Capture enabled at %.1f%%\n", newRate)
}

func (c *Controller) handleCaptureDisable(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	c.mu.Lock()
	c.captureRate = 0.0
	c.mu.Unlock()

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Capture disabled\n"))
}

func (c *Controller) handleCaptureRate(w http.ResponseWriter, r *http.Request) {
	c.mu.RLock()
	rate := c.captureRate
	c.mu.RUnlock()

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "%.1f\n", rate*100)
}

func (c *Controller) handleStatus(w http.ResponseWriter, r *http.Request) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	status := map[string]interface{}{
		"version":      c.version,
		"capture_rate": c.captureRate * 100,
		"project_id":   c.config.ProjectID,
		"zone":         c.config.Zone,
		"timestamp":    time.Now().UTC(),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

func parseInstanceURL(instanceURL string) []string {
	// Parse instance URL: projects/PROJECT/zones/ZONE/instances/INSTANCE
	parts := strings.Split(instanceURL, "/")
	if len(parts) >= 6 {
		return []string{parts[3], parts[5]} // [zone, instance]
	}
	return nil
}