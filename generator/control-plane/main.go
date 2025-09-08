package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
)

var (
	setupLog = ctrl.Log.WithName("setup")

	// Prometheus metrics
	activeScenarios = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "loadgen_active_scenarios",
		Help: "Number of active load scenarios",
	})

	activeWorkers = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "loadgen_active_workers",
		Help: "Number of active worker pods",
	})

	recipesLoaded = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "loadgen_recipes_loaded",
		Help: "Number of recipes currently loaded",
	})

	scenarioErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "loadgen_scenario_errors_total",
			Help: "Total number of scenario errors",
		},
		[]string{"scenario", "error_type"},
	)
)

func init() {
	prometheus.MustRegister(activeScenarios)
	prometheus.MustRegister(activeWorkers)
	prometheus.MustRegister(recipesLoaded)
	prometheus.MustRegister(scenarioErrors)
}

// LoadScenario represents a load generation scenario
type LoadScenario struct {
	Name        string                 `json:"name" yaml:"name"`
	Namespace   string                 `json:"namespace" yaml:"namespace"`
	Spec        LoadScenarioSpec       `json:"spec" yaml:"spec"`
	Status      LoadScenarioStatus     `json:"status" yaml:"status"`
	Metadata    map[string]interface{} `json:"metadata,omitempty" yaml:"metadata,omitempty"`
}

type LoadScenarioSpec struct {
	// Target configuration
	Families    []string `json:"families" yaml:"families"`        // Family patterns/globs
	Multiplier  float64  `json:"multiplier" yaml:"multiplier"`    // Load multiplier (1.0 = original scale)
	
	// Traffic shaping
	BurstFactor    float64            `json:"burstFactor,omitempty" yaml:"burstFactor,omitempty"`
	SchemaDrift    float64            `json:"schemaDrift,omitempty" yaml:"schemaDrift,omitempty"`
	ErrorInjection float64            `json:"errorInjection,omitempty" yaml:"errorInjection,omitempty"`
	TagSkew        map[string]float64 `json:"tagSkew,omitempty" yaml:"tagSkew,omitempty"`
	
	// Resource allocation
	WorkerPods    int32  `json:"workerPods" yaml:"workerPods"`
	WorkerCPU     string `json:"workerCPU,omitempty" yaml:"workerCPU,omitempty"`
	WorkerMemory  string `json:"workerMemory,omitempty" yaml:"workerMemory,omitempty"`
	
	// Duration and scheduling
	Duration      *string `json:"duration,omitempty" yaml:"duration,omitempty"`
	Schedule      *string `json:"schedule,omitempty" yaml:"schedule,omitempty"`
	
	// Target endpoints (reuse from old loadgen)
	Endpoints     []string `json:"endpoints" yaml:"endpoints"`
	Authentication map[string]string `json:"authentication,omitempty" yaml:"authentication,omitempty"`
}

type LoadScenarioStatus struct {
	Phase        string    `json:"phase" yaml:"phase"` // Pending, Running, Succeeded, Failed
	StartTime    *time.Time `json:"startTime,omitempty" yaml:"startTime,omitempty"`
	EndTime      *time.Time `json:"endTime,omitempty" yaml:"endTime,omitempty"`
	WorkerCount  int32     `json:"workerCount" yaml:"workerCount"`
	RecipeCount  int       `json:"recipeCount" yaml:"recipeCount"`
	Message      string    `json:"message,omitempty" yaml:"message,omitempty"`
	ErrorCount   int64     `json:"errorCount" yaml:"errorCount"`
	BytesEmitted int64     `json:"bytesEmitted" yaml:"bytesEmitted"`
	
	// Per-family status
	FamilyStatus map[string]FamilyStatus `json:"familyStatus,omitempty" yaml:"familyStatus,omitempty"`
}

type FamilyStatus struct {
	Assigned     bool    `json:"assigned" yaml:"assigned"`
	RecipeLoaded bool    `json:"recipeLoaded" yaml:"recipeLoaded"`
	EmissionRate float64 `json:"emissionRate" yaml:"emissionRate"` // bytes/sec
	ErrorRate    float64 `json:"errorRate" yaml:"errorRate"`
	Divergence   float64 `json:"divergence" yaml:"divergence"`
}

// Recipe represents a loaded metric family recipe
type Recipe struct {
	FamilyID    string                 `json:"family_id"`
	MetricName  string                 `json:"metric_name"`
	Version     string                 `json:"version"`
	Schema      map[string]interface{} `json:"schema"`
	Statistics  map[string]interface{} `json:"statistics"`
	Temporal    map[string]interface{} `json:"temporal"`
	Patterns    map[string]interface{} `json:"patterns"`
	Generation  map[string]interface{} `json:"generation"`
	LoadedAt    time.Time              `json:"loaded_at"`
}

// WorkerAssignment represents a recipe assignment to a worker pod
type WorkerAssignment struct {
	WorkerID     string    `json:"worker_id"`
	PodName      string    `json:"pod_name"`
	Namespace    string    `json:"namespace"`
	Families     []string  `json:"families"`
	Multiplier   float64   `json:"multiplier"`
	BurstFactor  float64   `json:"burst_factor"`
	AssignedAt   time.Time `json:"assigned_at"`
}

// ControlPlane manages load scenarios and worker coordination
type ControlPlane struct {
	k8sClient     client.Client
	gcsClient     *storage.Client
	recipeCache   map[string]*Recipe
	scenarios     map[string]*LoadScenario
	assignments   map[string]*WorkerAssignment
	mu            sync.RWMutex
	recipeBucket  string
	recipePrefix  string
}

func NewControlPlane(recipeBucket, recipePrefix string) (*ControlPlane, error) {
	// Initialize Kubernetes client
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to get k8s config: %w", err)
	}

	scheme := runtime.NewScheme()
	k8sClient, err := client.New(config, client.Options{Scheme: scheme})
	if err != nil {
		return nil, fmt.Errorf("failed to create k8s client: %w", err)
	}

	// Initialize GCS client
	gcsClient, err := storage.NewClient(context.Background(), option.WithScopes(storage.ScopeReadOnly))
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS client: %w", err)
	}

	return &ControlPlane{
		k8sClient:     k8sClient,
		gcsClient:     gcsClient,
		recipeCache:   make(map[string]*Recipe),
		scenarios:     make(map[string]*LoadScenario),
		assignments:   make(map[string]*WorkerAssignment),
		recipeBucket:  recipeBucket,
		recipePrefix:  recipePrefix,
	}, nil
}

func (cp *ControlPlane) Start(ctx context.Context, port int, metricsPort int) error {
	log.Printf("Starting control plane on port %d", port)

	// Start metrics server
	go cp.startMetricsServer(metricsPort)

	// Start recipe loader
	go cp.recipeLoaderLoop(ctx)

	// Start scenario reconciler
	go cp.scenarioReconcilerLoop(ctx)

	// Start worker health checker
	go cp.workerHealthLoop(ctx)

	// Start HTTP API server
	return cp.startHTTPServer(ctx, port)
}

func (cp *ControlPlane) startHTTPServer(ctx context.Context, port int) error {
	router := mux.NewRouter()

	// API routes
	api := router.PathPrefix("/api/v1").Subrouter()
	api.HandleFunc("/scenarios", cp.handleListScenarios).Methods("GET")
	api.HandleFunc("/scenarios", cp.handleCreateScenario).Methods("POST")
	api.HandleFunc("/scenarios/{name}", cp.handleGetScenario).Methods("GET")
	api.HandleFunc("/scenarios/{name}", cp.handleUpdateScenario).Methods("PUT")
	api.HandleFunc("/scenarios/{name}", cp.handleDeleteScenario).Methods("DELETE")
	
	// Recipe management
	api.HandleFunc("/recipes", cp.handleListRecipes).Methods("GET")
	api.HandleFunc("/recipes/{family_id}", cp.handleGetRecipe).Methods("GET")
	api.HandleFunc("/recipes/reload", cp.handleReloadRecipes).Methods("POST")
	
	// Worker management
	api.HandleFunc("/workers", cp.handleListWorkers).Methods("GET")
	api.HandleFunc("/workers/{id}/assignment", cp.handleWorkerAssignment).Methods("GET", "PUT")
	
	// Health and status
	router.HandleFunc("/health", cp.handleHealth).Methods("GET")
	router.HandleFunc("/ready", cp.handleReady).Methods("GET")
	router.HandleFunc("/status", cp.handleStatus).Methods("GET")

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: router,
	}

	go func() {
		<-ctx.Done()
		log.Println("Shutting down HTTP server...")
		server.Shutdown(context.Background())
	}()

	log.Printf("HTTP API server listening on port %d", port)
	return server.ListenAndServe()
}

func (cp *ControlPlane) startMetricsServer(port int) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	log.Printf("Metrics server listening on port %d", port)
	if err := server.ListenAndServe(); err != nil {
		log.Printf("Metrics server error: %v", err)
	}
}

func (cp *ControlPlane) handleListScenarios(w http.ResponseWriter, r *http.Request) {
	cp.mu.RLock()
	scenarios := make([]*LoadScenario, 0, len(cp.scenarios))
	for _, scenario := range cp.scenarios {
		scenarios = append(scenarios, scenario)
	}
	cp.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(scenarios)
}

func (cp *ControlPlane) handleCreateScenario(w http.ResponseWriter, r *http.Request) {
	var scenario LoadScenario
	if err := json.NewDecoder(r.Body).Decode(&scenario); err != nil {
		http.Error(w, fmt.Sprintf("Invalid JSON: %v", err), http.StatusBadRequest)
		return
	}

	// Validate scenario
	if err := cp.validateScenario(&scenario); err != nil {
		http.Error(w, fmt.Sprintf("Invalid scenario: %v", err), http.StatusBadRequest)
		return
	}

	// Initialize status
	scenario.Status = LoadScenarioStatus{
		Phase: "Pending",
	}

	cp.mu.Lock()
	cp.scenarios[scenario.Name] = &scenario
	cp.mu.Unlock()

	activeScenarios.Inc()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(&scenario)
}

func (cp *ControlPlane) handleGetScenario(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]

	cp.mu.RLock()
	scenario, exists := cp.scenarios[name]
	cp.mu.RUnlock()

	if !exists {
		http.Error(w, "Scenario not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(scenario)
}

func (cp *ControlPlane) handleUpdateScenario(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]

	var updates LoadScenario
	if err := json.NewDecoder(r.Body).Decode(&updates); err != nil {
		http.Error(w, fmt.Sprintf("Invalid JSON: %v", err), http.StatusBadRequest)
		return
	}

	cp.mu.Lock()
	scenario, exists := cp.scenarios[name]
	if !exists {
		cp.mu.Unlock()
		http.Error(w, "Scenario not found", http.StatusNotFound)
		return
	}

	// Update allowed fields
	scenario.Spec.Multiplier = updates.Spec.Multiplier
	scenario.Spec.BurstFactor = updates.Spec.BurstFactor
	scenario.Spec.SchemaDrift = updates.Spec.SchemaDrift
	scenario.Spec.ErrorInjection = updates.Spec.ErrorInjection
	scenario.Spec.TagSkew = updates.Spec.TagSkew
	scenario.Spec.WorkerPods = updates.Spec.WorkerPods

	cp.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(scenario)
}

func (cp *ControlPlane) handleDeleteScenario(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]

	cp.mu.Lock()
	scenario, exists := cp.scenarios[name]
	if exists {
		delete(cp.scenarios, name)
		activeScenarios.Dec()
	}
	cp.mu.Unlock()

	if !exists {
		http.Error(w, "Scenario not found", http.StatusNotFound)
		return
	}

	// TODO: Clean up worker assignments

	w.WriteHeader(http.StatusNoContent)
}

func (cp *ControlPlane) handleListRecipes(w http.ResponseWriter, r *http.Request) {
	cp.mu.RLock()
	recipes := make([]*Recipe, 0, len(cp.recipeCache))
	for _, recipe := range cp.recipeCache {
		recipes = append(recipes, recipe)
	}
	cp.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(recipes)
}

func (cp *ControlPlane) handleGetRecipe(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	familyID := vars["family_id"]

	cp.mu.RLock()
	recipe, exists := cp.recipeCache[familyID]
	cp.mu.RUnlock()

	if !exists {
		http.Error(w, "Recipe not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(recipe)
}

func (cp *ControlPlane) handleReloadRecipes(w http.ResponseWriter, r *http.Request) {
	go cp.loadRecipes(context.Background())
	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte("Recipe reload initiated"))
}

func (cp *ControlPlane) handleListWorkers(w http.ResponseWriter, r *http.Request) {
	cp.mu.RLock()
	assignments := make([]*WorkerAssignment, 0, len(cp.assignments))
	for _, assignment := range cp.assignments {
		assignments = append(assignments, assignment)
	}
	cp.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(assignments)
}

func (cp *ControlPlane) handleWorkerAssignment(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	workerID := vars["id"]

	if r.Method == "GET" {
		cp.mu.RLock()
		assignment, exists := cp.assignments[workerID]
		cp.mu.RUnlock()

		if !exists {
			http.Error(w, "Worker not found", http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(assignment)
	} else if r.Method == "PUT" {
		var assignment WorkerAssignment
		if err := json.NewDecoder(r.Body).Decode(&assignment); err != nil {
			http.Error(w, fmt.Sprintf("Invalid JSON: %v", err), http.StatusBadRequest)
			return
		}

		assignment.WorkerID = workerID
		assignment.AssignedAt = time.Now()

		cp.mu.Lock()
		cp.assignments[workerID] = &assignment
		cp.mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(&assignment)
	}
}

func (cp *ControlPlane) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func (cp *ControlPlane) handleReady(w http.ResponseWriter, r *http.Request) {
	// Check if essential components are ready
	cp.mu.RLock()
	recipeCount := len(cp.recipeCache)
	cp.mu.RUnlock()

	if recipeCount == 0 {
		http.Error(w, "No recipes loaded", http.StatusServiceUnavailable)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("READY"))
}

func (cp *ControlPlane) handleStatus(w http.ResponseWriter, r *http.Request) {
	cp.mu.RLock()
	status := map[string]interface{}{
		"scenarios":     len(cp.scenarios),
		"recipes":       len(cp.recipeCache),
		"workers":       len(cp.assignments),
		"timestamp":     time.Now().UTC(),
	}
	cp.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

func (cp *ControlPlane) validateScenario(scenario *LoadScenario) error {
	if scenario.Name == "" {
		return fmt.Errorf("scenario name is required")
	}
	if scenario.Spec.Multiplier <= 0 {
		return fmt.Errorf("multiplier must be positive")
	}
	if scenario.Spec.WorkerPods <= 0 {
		return fmt.Errorf("worker pods must be positive")
	}
	if len(scenario.Spec.Endpoints) == 0 {
		return fmt.Errorf("at least one endpoint is required")
	}
	return nil
}

func (cp *ControlPlane) recipeLoaderLoop(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()

	// Initial load
	cp.loadRecipes(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cp.loadRecipes(ctx)
		}
	}
}

func (cp *ControlPlane) loadRecipes(ctx context.Context) {
	log.Println("Loading recipes from GCS...")

	bucket := cp.gcsClient.Bucket(cp.recipeBucket)
	it := bucket.Objects(ctx, &storage.Query{
		Prefix: cp.recipePrefix,
	})

	loadedCount := 0
	for {
		attrs, err := it.Next()
		if err != nil {
			break // Done or error
		}

		if !strings.HasSuffix(attrs.Name, ".json.zst") {
			continue
		}

		// Extract family ID from filename
		filename := filepath.Base(attrs.Name)
		familyID := strings.TrimSuffix(filename, ".json.zst")

		if recipe, err := cp.loadRecipe(ctx, attrs.Name); err != nil {
			log.Printf("Failed to load recipe %s: %v", familyID, err)
		} else {
			cp.mu.Lock()
			cp.recipeCache[familyID] = recipe
			cp.mu.Unlock()
			loadedCount++
		}
	}

	recipesLoaded.Set(float64(loadedCount))
	log.Printf("Loaded %d recipes", loadedCount)
}

func (cp *ControlPlane) loadRecipe(ctx context.Context, objectName string) (*Recipe, error) {
	obj := cp.gcsClient.Bucket(cp.recipeBucket).Object(objectName)
	reader, err := obj.NewReader(ctx)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	// TODO: Add zstd decompression
	var recipe Recipe
	if err := json.NewDecoder(reader).Decode(&recipe); err != nil {
		return nil, err
	}

	recipe.LoadedAt = time.Now()
	return &recipe, nil
}

func (cp *ControlPlane) scenarioReconcilerLoop(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cp.reconcileScenarios(ctx)
		}
	}
}

func (cp *ControlPlane) reconcileScenarios(ctx context.Context) {
	cp.mu.RLock()
	scenarios := make([]*LoadScenario, 0, len(cp.scenarios))
	for _, scenario := range cp.scenarios {
		scenarios = append(scenarios, scenario)
	}
	cp.mu.RUnlock()

	for _, scenario := range scenarios {
		if err := cp.reconcileScenario(ctx, scenario); err != nil {
			log.Printf("Failed to reconcile scenario %s: %v", scenario.Name, err)
			scenarioErrors.WithLabelValues(scenario.Name, "reconcile_error").Inc()
		}
	}
}

func (cp *ControlPlane) reconcileScenario(ctx context.Context, scenario *LoadScenario) error {
	// TODO: Implement scenario reconciliation
	// - Ensure worker pods are running
	// - Distribute recipe assignments
	// - Monitor worker health
	// - Update scenario status

	return nil
}

func (cp *ControlPlane) workerHealthLoop(ctx context.Context) {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cp.checkWorkerHealth(ctx)
		}
	}
}

func (cp *ControlPlane) checkWorkerHealth(ctx context.Context) {
	cp.mu.RLock()
	assignments := make([]*WorkerAssignment, 0, len(cp.assignments))
	for _, assignment := range cp.assignments {
		assignments = append(assignments, assignment)
	}
	cp.mu.RUnlock()

	healthyWorkers := 0
	for _, assignment := range assignments {
		if cp.isWorkerHealthy(ctx, assignment) {
			healthyWorkers++
		}
	}

	activeWorkers.Set(float64(healthyWorkers))
}

func (cp *ControlPlane) isWorkerHealthy(ctx context.Context, assignment *WorkerAssignment) bool {
	// TODO: Check worker pod health via Kubernetes API
	return true
}

func main() {
	var (
		port         = flag.Int("port", 8080, "HTTP port")
		metricsPort  = flag.Int("metrics-port", 9090, "Metrics port")
		recipeBucket = flag.String("recipe-bucket", "", "GCS bucket for recipes")
		recipePrefix = flag.String("recipe-prefix", "recipes/v1", "GCS prefix for recipes")
	)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))

	if *recipeBucket == "" {
		log.Fatal("recipe-bucket is required")
	}

	cp, err := NewControlPlane(*recipeBucket, *recipePrefix)
	if err != nil {
		log.Fatalf("Failed to create control plane: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		log.Println("Shutting down...")
		cancel()
	}()

	if err := cp.Start(ctx, *port, *metricsPort); err != nil {
		log.Fatalf("Control plane failed: %v", err)
	}
}