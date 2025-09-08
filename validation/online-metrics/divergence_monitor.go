package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gonum.org/v1/gonum/stat"
)

// DivergenceMonitor tracks statistical divergence between generated and reference data
type DivergenceMonitor struct {
	families        map[string]*FamilyMonitor
	referencePath   string
	mu              sync.RWMutex
	alertThresholds AlertThresholds
}

type AlertThresholds struct {
	JSThreshold           float64 // Jensen-Shannon divergence threshold
	WassersteinThreshold  float64 // Wasserstein distance threshold  
	KSThreshold           float64 // Kolmogorov-Smirnov threshold
	RedStatusMinutes      int     // Minutes before alerting on red status
}

type FamilyMonitor struct {
	FamilyID           string
	MetricName         string
	ReferenceStats     *ReferenceStatistics
	CurrentWindow      *SlidingWindow
	DivergenceScores   *DivergenceScores
	LastUpdate         time.Time
	Status             string // green, amber, red
	ConsecutiveRed     int
	mu                 sync.RWMutex
}

type ReferenceStatistics struct {
	// Categorical distributions (for tags, sources)
	SourceDistribution    map[string]float64
	TagDistributions      map[string]map[string]float64
	
	// Numeric distributions (for values)
	ValueQuantiles        []float64
	ValueHistogram        []HistogramBin
	
	// Temporal patterns
	IntensityCurve        []float64
	BurstinessMean        float64
	BurstinessStdDev      float64
	
	// Co-occurrence patterns
	TagCooccurrence       map[string]float64
	
	// Size distribution  
	SizeQuantiles         []float64
}

type HistogramBin struct {
	LowerBound float64
	UpperBound float64
	Count      int
	Density    float64
}

type SlidingWindow struct {
	WindowSize    time.Duration
	Samples       []Sample
	maxSamples    int
	mu            sync.Mutex
}

type Sample struct {
	Timestamp    time.Time
	Value        float64
	Source       string
	Tags         map[string]string
	LineSize     int
}

type DivergenceScores struct {
	JSCategorical     float64
	WassersteinValue  float64
	KSSize           float64
	TemporalCorr     float64
	CooccurrenceJS   float64
	LastCalculated   time.Time
}

var (
	// Prometheus metrics for divergence monitoring
	divergenceJS = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "loadgen_divergence_jensen_shannon",
			Help: "Jensen-Shannon divergence for categorical distributions",
		},
		[]string{"family_id", "distribution_type"},
	)

	divergenceWasserstein = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "loadgen_divergence_wasserstein",
			Help: "Wasserstein distance for numeric distributions",  
		},
		[]string{"family_id"},
	)

	divergenceKS = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "loadgen_divergence_kolmogorov_smirnov",
			Help: "Kolmogorov-Smirnov statistic for size distributions",
		},
		[]string{"family_id"},
	)

	familyStatus = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "loadgen_family_status",
			Help: "Family status: 0=green, 1=amber, 2=red",
		},
		[]string{"family_id", "metric_name"},
	)

	alertsActive = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "loadgen_alerts_active",
			Help: "Number of active alerts",
		},
		[]string{"severity", "type"},
	)
)

func init() {
	prometheus.MustRegister(divergenceJS)
	prometheus.MustRegister(divergenceWasserstein)
	prometheus.MustRegister(divergenceKS)
	prometheus.MustRegister(familyStatus)
	prometheus.MustRegister(alertsActive)
}

func NewDivergenceMonitor(referencePath string) *DivergenceMonitor {
	return &DivergenceMonitor{
		families:      make(map[string]*FamilyMonitor),
		referencePath: referencePath,
		alertThresholds: AlertThresholds{
			JSThreshold:          0.05,
			WassersteinThreshold: 0.1,
			KSThreshold:          0.05,
			RedStatusMinutes:     15,
		},
	}
}

func (dm *DivergenceMonitor) LoadReferences(ctx context.Context) error {
	log.Println("Loading reference statistics...")
	
	// Load reference statistics from GCS or local file
	// This would parse the Recipe files and extract reference distributions
	
	// For now, create mock reference data
	mockFamily := &FamilyMonitor{
		FamilyID:   "mock-family-123",
		MetricName: "test.metric",
		ReferenceStats: &ReferenceStatistics{
			SourceDistribution: map[string]float64{
				"host-001": 0.3,
				"host-002": 0.2,
				"host-003": 0.5,
			},
			TagDistributions: map[string]map[string]float64{
				"env": {
					"prod":    0.7,
					"staging": 0.2, 
					"dev":     0.1,
				},
				"region": {
					"us-east-1": 0.4,
					"us-west-2": 0.3,
					"eu-west-1": 0.3,
				},
			},
			ValueQuantiles:   []float64{1.0, 10.0, 50.0, 90.0, 99.0},
			IntensityCurve:   generateMockIntensityCurve(),
			BurstinessMean:   1.2,
			BurstinessStdDev: 0.3,
			SizeQuantiles:    []float64{80, 120, 200, 350, 500},
		},
		CurrentWindow: NewSlidingWindow(5 * time.Minute),
		DivergenceScores: &DivergenceScores{},
		Status: "green",
	}
	
	dm.mu.Lock()
	dm.families[mockFamily.FamilyID] = mockFamily
	dm.mu.Unlock()
	
	log.Printf("Loaded references for %d families", len(dm.families))
	return nil
}

func (dm *DivergenceMonitor) Start(ctx context.Context, port int) error {
	// Start metrics server
	go dm.startMetricsServer(port)
	
	// Start monitoring loop
	go dm.monitoringLoop(ctx)
	
	// Start HTTP server for manual triggers
	return dm.startHTTPServer(ctx, port+1)
}

func (dm *DivergenceMonitor) startMetricsServer(port int) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	log.Printf("Divergence metrics server listening on port %d", port)
	if err := server.ListenAndServe(); err != nil {
		log.Printf("Metrics server error: %v", err)
	}
}

func (dm *DivergenceMonitor) startHTTPServer(ctx context.Context, port int) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", dm.handleHealth)
	mux.HandleFunc("/status", dm.handleStatus)
	mux.HandleFunc("/families", dm.handleFamilies)
	mux.HandleFunc("/families/{id}/divergence", dm.handleFamilyDivergence)
	mux.HandleFunc("/compute", dm.handleComputeDivergence)

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	go func() {
		<-ctx.Done()
		server.Shutdown(context.Background())
	}()

	log.Printf("Divergence HTTP server listening on port %d", port)
	return server.ListenAndServe()
}

func (dm *DivergenceMonitor) monitoringLoop(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			dm.computeAllDivergences()
			dm.updateAlertStatus()
		}
	}
}

func (dm *DivergenceMonitor) computeAllDivergences() {
	dm.mu.RLock()
	families := make([]*FamilyMonitor, 0, len(dm.families))
	for _, family := range dm.families {
		families = append(families, family)
	}
	dm.mu.RUnlock()

	for _, family := range families {
		dm.computeFamilyDivergence(family)
	}
}

func (dm *DivergenceMonitor) computeFamilyDivergence(family *FamilyMonitor) {
	family.mu.Lock()
	defer family.mu.Unlock()

	if len(family.CurrentWindow.Samples) < 10 {
		return // Need minimum samples
	}

	// Compute categorical divergences (JS)
	jsSource := dm.computeJSDivergence(
		family.ReferenceStats.SourceDistribution,
		dm.extractSourceDistribution(family.CurrentWindow.Samples),
	)

	jsTagAvg := 0.0
	tagCount := 0
	for tagKey, refDist := range family.ReferenceStats.TagDistributions {
		currentDist := dm.extractTagDistribution(family.CurrentWindow.Samples, tagKey)
		jsTag := dm.computeJSDivergence(refDist, currentDist)
		jsTagAvg += jsTag
		tagCount++
		
		// Update individual tag metrics
		divergenceJS.WithLabelValues(family.FamilyID, fmt.Sprintf("tag_%s", tagKey)).Set(jsTag)
	}
	if tagCount > 0 {
		jsTagAvg /= float64(tagCount)
	}

	divergenceJS.WithLabelValues(family.FamilyID, "source").Set(jsSource)
	divergenceJS.WithLabelValues(family.FamilyID, "tags_average").Set(jsTagAvg)

	// Compute numeric divergence (Wasserstein)
	currentValues := dm.extractValues(family.CurrentWindow.Samples)
	wasserstein := dm.computeWassersteinDistance(
		family.ReferenceStats.ValueQuantiles,
		dm.computeQuantiles(currentValues, []float64{0.01, 0.05, 0.5, 0.95, 0.99}),
	)
	divergenceWasserstein.WithLabelValues(family.FamilyID).Set(wasserstein)

	// Compute size distribution divergence (KS)
	currentSizes := dm.extractSizes(family.CurrentWindow.Samples)
	ks := dm.computeKSStatistic(
		family.ReferenceStats.SizeQuantiles,
		dm.computeQuantiles(currentSizes, []float64{0.01, 0.05, 0.5, 0.95, 0.99}),
	)
	divergenceKS.WithLabelValues(family.FamilyID).Set(ks)

	// Update family divergence scores
	family.DivergenceScores.JSCategorical = (jsSource + jsTagAvg) / 2.0
	family.DivergenceScores.WassersteinValue = wasserstein
	family.DivergenceScores.KSSize = ks
	family.DivergenceScores.LastCalculated = time.Now()

	// Determine status
	family.Status = dm.determineStatus(family.DivergenceScores)
	
	// Update status metric
	statusValue := 0.0
	switch family.Status {
	case "amber":
		statusValue = 1.0
	case "red":
		statusValue = 2.0
		family.ConsecutiveRed++
	default:
		family.ConsecutiveRed = 0
	}
	familyStatus.WithLabelValues(family.FamilyID, family.MetricName).Set(statusValue)

	log.Printf("Family %s: JS=%.3f, Wasserstein=%.3f, KS=%.3f, Status=%s",
		family.FamilyID[:8], family.DivergenceScores.JSCategorical,
		family.DivergenceScores.WassersteinValue, family.DivergenceScores.KSSize,
		family.Status)
}

func (dm *DivergenceMonitor) determineStatus(scores *DivergenceScores) string {
	// Red thresholds
	if scores.JSCategorical > dm.alertThresholds.JSThreshold ||
	   scores.WassersteinValue > dm.alertThresholds.WassersteinThreshold ||
	   scores.KSSize > dm.alertThresholds.KSThreshold {
		return "red"
	}

	// Amber thresholds (50% of red thresholds)  
	if scores.JSCategorical > dm.alertThresholds.JSThreshold*0.5 ||
	   scores.WassersteinValue > dm.alertThresholds.WassersteinThreshold*0.5 ||
	   scores.KSSize > dm.alertThresholds.KSThreshold*0.5 {
		return "amber"
	}

	return "green"
}

func (dm *DivergenceMonitor) updateAlertStatus() {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	redCount := 0
	amberCount := 0
	criticalAlerts := 0

	for _, family := range dm.families {
		family.mu.RLock()
		status := family.Status
		consecutiveRed := family.ConsecutiveRed
		family.mu.RUnlock()

		switch status {
		case "red":
			redCount++
			if consecutiveRed >= dm.alertThresholds.RedStatusMinutes {
				criticalAlerts++
			}
		case "amber":
			amberCount++
		}
	}

	alertsActive.WithLabelValues("critical", "divergence").Set(float64(criticalAlerts))
	alertsActive.WithLabelValues("warning", "divergence").Set(float64(amberCount))
	alertsActive.WithLabelValues("info", "divergence").Set(float64(redCount))
}

// Statistical computation methods

func (dm *DivergenceMonitor) computeJSDivergence(ref, current map[string]float64) float64 {
	// Jensen-Shannon divergence computation
	if len(ref) == 0 || len(current) == 0 {
		return 1.0 // Maximum divergence
	}

	// Get all keys
	allKeys := make(map[string]bool)
	for k := range ref {
		allKeys[k] = true
	}
	for k := range current {
		allKeys[k] = true
	}

	// Compute JS divergence
	js := 0.0
	for key := range allKeys {
		p := ref[key]
		q := current[key]
		
		if p == 0 && q == 0 {
			continue
		}
		
		m := (p + q) / 2.0
		if p > 0 && m > 0 {
			js += p * math.Log(p/m)
		}
		if q > 0 && m > 0 {
			js += q * math.Log(q/m)
		}
	}
	
	return js / (2.0 * math.Log(2.0)) // Normalize to [0,1]
}

func (dm *DivergenceMonitor) computeWassersteinDistance(refQuantiles, currentQuantiles []float64) float64 {
	if len(refQuantiles) == 0 || len(currentQuantiles) == 0 {
		return 1.0
	}

	// Simplified 1-Wasserstein distance using quantiles
	distance := 0.0
	minLen := len(refQuantiles)
	if len(currentQuantiles) < minLen {
		minLen = len(currentQuantiles)
	}

	for i := 0; i < minLen; i++ {
		distance += math.Abs(refQuantiles[i] - currentQuantiles[i])
	}

	// Normalize by range
	refRange := refQuantiles[len(refQuantiles)-1] - refQuantiles[0]
	if refRange > 0 {
		distance /= refRange
	}

	return distance / float64(minLen)
}

func (dm *DivergenceMonitor) computeKSStatistic(refQuantiles, currentQuantiles []float64) float64 {
	if len(refQuantiles) == 0 || len(currentQuantiles) == 0 {
		return 1.0
	}

	// Simplified KS statistic using quantiles
	maxDiff := 0.0
	minLen := len(refQuantiles)
	if len(currentQuantiles) < minLen {
		minLen = len(currentQuantiles)
	}

	for i := 0; i < minLen; i++ {
		// Approximate CDF difference at quantile points
		diff := math.Abs(float64(i)/float64(minLen) - float64(i)/float64(minLen))
		if diff > maxDiff {
			maxDiff = diff
		}
	}

	return maxDiff
}

func (dm *DivergenceMonitor) computeQuantiles(values []float64, quantiles []float64) []float64 {
	if len(values) == 0 {
		return make([]float64, len(quantiles))
	}

	sort.Float64s(values)
	result := make([]float64, len(quantiles))
	
	for i, q := range quantiles {
		pos := q * float64(len(values)-1)
		idx := int(pos)
		if idx >= len(values)-1 {
			result[i] = values[len(values)-1]
		} else {
			// Linear interpolation
			frac := pos - float64(idx)
			result[i] = values[idx] + frac*(values[idx+1]-values[idx])
		}
	}

	return result
}

// Data extraction methods

func (dm *DivergenceMonitor) extractSourceDistribution(samples []Sample) map[string]float64 {
	counts := make(map[string]int)
	total := 0
	
	for _, sample := range samples {
		counts[sample.Source]++
		total++
	}

	dist := make(map[string]float64)
	for source, count := range counts {
		dist[source] = float64(count) / float64(total)
	}
	
	return dist
}

func (dm *DivergenceMonitor) extractTagDistribution(samples []Sample, tagKey string) map[string]float64 {
	counts := make(map[string]int)
	total := 0
	
	for _, sample := range samples {
		if tagValue, exists := sample.Tags[tagKey]; exists {
			counts[tagValue]++
			total++
		}
	}

	dist := make(map[string]float64)
	for value, count := range counts {
		dist[value] = float64(count) / float64(total)
	}
	
	return dist
}

func (dm *DivergenceMonitor) extractValues(samples []Sample) []float64 {
	values := make([]float64, len(samples))
	for i, sample := range samples {
		values[i] = sample.Value
	}
	return values
}

func (dm *DivergenceMonitor) extractSizes(samples []Sample) []float64 {
	sizes := make([]float64, len(samples))
	for i, sample := range samples {
		sizes[i] = float64(sample.LineSize)
	}
	return sizes
}

// HTTP handlers

func (dm *DivergenceMonitor) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func (dm *DivergenceMonitor) handleStatus(w http.ResponseWriter, r *http.Request) {
	dm.mu.RLock()
	status := map[string]interface{}{
		"families":  len(dm.families),
		"timestamp": time.Now().UTC(),
	}
	dm.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

func (dm *DivergenceMonitor) handleFamilies(w http.ResponseWriter, r *http.Request) {
	dm.mu.RLock()
	families := make([]map[string]interface{}, 0, len(dm.families))
	for _, family := range dm.families {
		family.mu.RLock()
		families = append(families, map[string]interface{}{
			"family_id":    family.FamilyID,
			"metric_name":  family.MetricName,
			"status":       family.Status,
			"last_update":  family.LastUpdate,
			"samples":      len(family.CurrentWindow.Samples),
			"divergence":   family.DivergenceScores,
		})
		family.mu.RUnlock()
	}
	dm.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(families)
}

func (dm *DivergenceMonitor) handleFamilyDivergence(w http.ResponseWriter, r *http.Request) {
	// Extract family ID from URL path
	// Simplified implementation
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "not implemented"})
}

func (dm *DivergenceMonitor) handleComputeDivergence(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	dm.computeAllDivergences()
	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte("Divergence computation triggered"))
}

// Utility functions

func NewSlidingWindow(duration time.Duration) *SlidingWindow {
	return &SlidingWindow{
		WindowSize: duration,
		Samples:    make([]Sample, 0),
		maxSamples: 10000, // Limit memory usage
	}
}

func (sw *SlidingWindow) AddSample(sample Sample) {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	// Add new sample
	sw.Samples = append(sw.Samples, sample)

	// Remove old samples outside window
	cutoff := time.Now().Add(-sw.WindowSize)
	validStart := 0
	for i, s := range sw.Samples {
		if s.Timestamp.After(cutoff) {
			validStart = i
			break
		}
	}

	if validStart > 0 {
		sw.Samples = sw.Samples[validStart:]
	}

	// Limit total samples
	if len(sw.Samples) > sw.maxSamples {
		sw.Samples = sw.Samples[len(sw.Samples)-sw.maxSamples:]
	}
}

func generateMockIntensityCurve() []float64 {
	curve := make([]float64, 1440) // 24 hours * 60 minutes
	for i := range curve {
		// Simulate daily pattern with higher activity during business hours
		hour := float64(i) / 60.0
		if hour >= 8 && hour <= 18 {
			curve[i] = 1.5 + 0.5*math.Sin((hour-8)*math.Pi/10)
		} else {
			curve[i] = 0.3 + 0.2*math.Sin((hour)*math.Pi/12)
		}
	}
	return curve
}

func main() {
	var (
		port          = flag.Int("port", 9100, "Metrics port")
		referencePath = flag.String("reference-path", "gs://bucket/references", "Path to reference statistics")
	)
	flag.Parse()

	monitor := NewDivergenceMonitor(*referencePath)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Load references
	if err := monitor.LoadReferences(ctx); err != nil {
		log.Fatalf("Failed to load references: %v", err)
	}

	// Start monitoring
	if err := monitor.Start(ctx, *port); err != nil {
		log.Fatalf("Monitor failed: %v", err)
	}
}