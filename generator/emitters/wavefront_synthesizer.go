package emitters

import (
	"fmt"
	"math"
	"math/rand"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/loadgen/generator-lib/payload-synth"
)

// WavefrontSynthesizer generates realistic Wavefront lines from Recipes
type WavefrontSynthesizer struct {
	recipe           *Recipe
	rng              *rand.Rand
	tagSamplers      map[string]*payloadsynth.CategoricalSampler
	sourceSampler    *payloadsynth.CategoricalSampler
	valueSampler     *payloadsynth.NumericSampler
	intensityCurve   []float64
	currentMinute    int
	startTime        time.Time
	deltaAccumulator map[string]float64
	stringPatterns   map[string]*payloadsynth.StringPatternSampler
}

// Recipe represents a loaded Wavefront family recipe
type Recipe struct {
	FamilyID    string                 `json:"family_id"`
	MetricName  string                 `json:"metric_name"`
	Schema      map[string]interface{} `json:"schema"`
	Statistics  map[string]interface{} `json:"statistics"`
	Temporal    map[string]interface{} `json:"temporal"`
	Patterns    map[string]interface{} `json:"patterns"`
	Generation  map[string]interface{} `json:"generation"`
	Validation  map[string]interface{} `json:"validation"`
}

// NewWavefrontSynthesizer creates a new synthesizer for a given recipe
func NewWavefrontSynthesizer(recipe *Recipe, seed int64, startTime time.Time) (*WavefrontSynthesizer, error) {
	ws := &WavefrontSynthesizer{
		recipe:           recipe,
		rng:              rand.New(rand.NewSource(seed)),
		tagSamplers:      make(map[string]*payloadsynth.CategoricalSampler),
		startTime:        startTime,
		deltaAccumulator: make(map[string]float64),
		stringPatterns:   make(map[string]*payloadsynth.StringPatternSampler),
	}

	if err := ws.initializeSamplers(); err != nil {
		return nil, fmt.Errorf("failed to initialize samplers: %w", err)
	}

	return ws, nil
}

func (ws *WavefrontSynthesizer) initializeSamplers() error {
	stats, ok := ws.recipe.Statistics["statistics"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("invalid statistics format in recipe")
	}

	// Initialize source sampler
	if sourceDist, ok := stats["source_distribution"].(map[string]interface{}); ok {
		sampler, err := ws.createCategoricalSampler(sourceDist)
		if err != nil {
			return fmt.Errorf("failed to create source sampler: %w", err)
		}
		ws.sourceSampler = sampler
	}

	// Initialize tag samplers
	if tagDists, ok := stats["tag_distributions"].(map[string]interface{}); ok {
		for tagKey, dist := range tagDists {
			if distMap, ok := dist.(map[string]interface{}); ok {
				sampler, err := ws.createCategoricalSampler(distMap)
				if err != nil {
					return fmt.Errorf("failed to create tag sampler for %s: %w", tagKey, err)
				}
				ws.tagSamplers[tagKey] = sampler
			}
		}
	}

	// Initialize value sampler
	if valueDist, ok := stats["value_distribution"].(map[string]interface{}); ok {
		sampler, err := ws.createNumericSampler(valueDist)
		if err != nil {
			return fmt.Errorf("failed to create value sampler: %w", err)
		}
		ws.valueSampler = sampler
	}

	// Initialize intensity curve
	if temporal, ok := ws.recipe.Temporal["temporal"].(map[string]interface{}); ok {
		if curve, ok := temporal["intensity_curve"].([]interface{}); ok {
			ws.intensityCurve = make([]float64, len(curve))
			for i, v := range curve {
				if f, ok := v.(float64); ok {
					ws.intensityCurve[i] = f
				} else {
					ws.intensityCurve[i] = 1.0
				}
			}
		}
	}

	// Initialize string pattern samplers
	if patterns, ok := ws.recipe.Patterns["patterns"].(map[string]interface{}); ok {
		ws.initializeStringPatterns(patterns)
	}

	return nil
}

func (ws *WavefrontSynthesizer) createCategoricalSampler(dist map[string]interface{}) (*payloadsynth.CategoricalSampler, error) {
	topValues, ok := dist["top_values"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid top_values format")
	}

	var items []payloadsynth.WeightedItem
	for _, item := range topValues {
		if itemMap, ok := item.(map[string]interface{}); ok {
			value, _ := itemMap["value"].(string)
			frequency, _ := itemMap["frequency"].(float64)
			items = append(items, payloadsynth.WeightedItem{
				Value:  value,
				Weight: frequency,
			})
		}
	}

	return payloadsynth.NewCategoricalSampler(items), nil
}

func (ws *WavefrontSynthesizer) createNumericSampler(dist map[string]interface{}) (*payloadsynth.NumericSampler, error) {
	quantiles, ok := dist["quantiles"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid quantiles format")
	}

	// Extract percentiles
	p01, _ := quantiles["p01"].(float64)
	p05, _ := quantiles["p05"].(float64)
	p50, _ := quantiles["p50"].(float64)
	p95, _ := quantiles["p95"].(float64)
	p99, _ := quantiles["p99"].(float64)

	return payloadsynth.NewQuantileSampler([]float64{p01, p05, p50, p95, p99}), nil
}

func (ws *WavefrontSynthesizer) initializeStringPatterns(patterns map[string]interface{}) {
	// Source patterns
	if sourcePatterns, ok := patterns["source_patterns"].([]interface{}); ok {
		ws.stringPatterns["source"] = ws.createStringPatternSampler(sourcePatterns)
	}

	// Tag value patterns
	if tagPatterns, ok := patterns["tag_value_patterns"].(map[string]interface{}); ok {
		for tagKey, patterns := range tagPatterns {
			if patternList, ok := patterns.([]interface{}); ok {
				ws.stringPatterns[tagKey] = ws.createStringPatternSampler(patternList)
			}
		}
	}
}

func (ws *WavefrontSynthesizer) createStringPatternSampler(patterns []interface{}) *payloadsynth.StringPatternSampler {
	var weightedPatterns []payloadsynth.WeightedPattern
	
	for _, p := range patterns {
		if pMap, ok := p.(map[string]interface{}); ok {
			pattern, _ := pMap["pattern"].(string)
			frequency, _ := pMap["frequency"].(float64)
			weightedPatterns = append(weightedPatterns, payloadsynth.WeightedPattern{
				Pattern: pattern,
				Weight:  frequency,
			})
		}
	}

	return payloadsynth.NewStringPatternSampler(weightedPatterns)
}

// SynthesizeLine generates a single Wavefront metric line
func (ws *WavefrontSynthesizer) SynthesizeLine(currentTime time.Time, multiplier float64) (string, error) {
	// Check if this is a delta counter
	schema, ok := ws.recipe.Schema["schema"].(map[string]interface{})
	if !ok {
		return "", fmt.Errorf("invalid schema format")
	}
	
	isDelta, _ := schema["is_delta"].(bool)
	hasHistogram, _ := schema["has_histogram"].(bool)

	// Decide whether to generate metric or histogram
	if hasHistogram && ws.rng.Float64() < 0.1 { // 10% histogram probability
		return ws.synthesizeHistogram(currentTime, multiplier)
	}

	return ws.synthesizeMetric(currentTime, multiplier, isDelta)
}

func (ws *WavefrontSynthesizer) synthesizeMetric(currentTime time.Time, multiplier float64, isDelta bool) (string, error) {
	// Generate metric name with delta prefix if needed
	metricName := ws.recipe.MetricName
	if isDelta {
		metricName = "∆" + metricName
	}

	// Generate value
	var value float64
	if ws.valueSampler != nil {
		value = ws.valueSampler.Sample(ws.rng)
	} else {
		value = ws.rng.NormFloat64() * 10 + 50 // Default distribution
	}

	// Apply multiplier
	value *= multiplier

	// For delta counters, accumulate per-minute and emit deltas
	if isDelta {
		minuteKey := fmt.Sprintf("%d", currentTime.Unix()/60)
		ws.deltaAccumulator[minuteKey] += value
		value = ws.deltaAccumulator[minuteKey]
		// Reset accumulator for next period (simplified)
	}

	// Generate source
	source := ws.generateSource()

	// Generate tags
	tags := ws.generateTags()

	// Format timestamp (optional in Wavefront, but useful for testing)
	timestamp := currentTime.Unix()

	// Construct line: <metric> <value> [<timestamp>] source=<source> [<tags>]
	var line strings.Builder
	line.WriteString(ws.escapeMetricName(metricName))
	line.WriteString(" ")
	line.WriteString(ws.formatValue(value))
	line.WriteString(" ")
	line.WriteString(strconv.FormatInt(timestamp, 10))
	line.WriteString(" source=")
	line.WriteString(ws.escapeTagValue(source))

	for key, val := range tags {
		line.WriteString(" ")
		line.WriteString(key)
		line.WriteString("=")
		line.WriteString(ws.escapeTagValue(val))
	}

	return line.String(), nil
}

func (ws *WavefrontSynthesizer) synthesizeHistogram(currentTime time.Time, multiplier float64) (string, error) {
	// Generate histogram line: !M <timestamp> #<count> <centroid_count> <centroid_value> ...
	// Followed by metric line with source and tags

	granularity := "M" // Default to minute
	if ws.rng.Float64() < 0.2 {
		granularity = "H" // 20% hour
	} else if ws.rng.Float64() < 0.05 {
		granularity = "D" // 5% day
	}

	timestamp := currentTime.Unix()
	
	// Generate centroids (simplified)
	centroidCount := 1 + ws.rng.Intn(5) // 1-5 centroids
	totalCount := int(multiplier * float64(10+ws.rng.Intn(90))) // 10-100 base count

	var line strings.Builder
	line.WriteString("!")
	line.WriteString(granularity)
	line.WriteString(" ")
	line.WriteString(strconv.FormatInt(timestamp, 10))
	line.WriteString(" #")
	line.WriteString(strconv.Itoa(totalCount))

	// Generate centroids
	for i := 0; i < centroidCount; i++ {
		count := totalCount / centroidCount
		if i == centroidCount-1 {
			count = totalCount - (centroidCount-1)*count // Remainder
		}
		
		value := ws.rng.NormFloat64()*50 + 100 // Sample centroid value
		
		line.WriteString(" ")
		line.WriteString(strconv.Itoa(count))
		line.WriteString(" ")
		line.WriteString(ws.formatValue(value))
	}

	line.WriteString("\n")

	// Add metric line
	source := ws.generateSource()
	tags := ws.generateTags()

	line.WriteString(ws.escapeMetricName(ws.recipe.MetricName))
	line.WriteString(" source=")
	line.WriteString(ws.escapeTagValue(source))

	for key, val := range tags {
		line.WriteString(" ")
		line.WriteString(key)
		line.WriteString("=")
		line.WriteString(ws.escapeTagValue(val))
	}

	return line.String(), nil
}

func (ws *WavefrontSynthesizer) generateSource() string {
	if ws.sourceSampler != nil {
		return ws.sourceSampler.Sample(ws.rng)
	}

	// Generate using pattern if available
	if sampler, ok := ws.stringPatterns["source"]; ok {
		return sampler.Generate(ws.rng)
	}

	// Default synthetic source
	return fmt.Sprintf("host-%d", ws.rng.Intn(1000))
}

func (ws *WavefrontSynthesizer) generateTags() map[string]string {
	tags := make(map[string]string)

	// Sample from each tag distribution based on presence probability
	schema, ok := ws.recipe.Schema["schema"].(map[string]interface{})
	if !ok {
		return tags
	}

	tagSchema, ok := schema["tag_schema"].(map[string]interface{})
	if !ok {
		return tags
	}

	for tagKey, schemaInfo := range tagSchema {
		if schemaMap, ok := schemaInfo.(map[string]interface{}); ok {
			presence, _ := schemaMap["presence"].(float64)
			
			// Decide whether to include this tag
			if ws.rng.Float64() < presence {
				value := ws.generateTagValue(tagKey)
				if value != "" {
					tags[tagKey] = value
				}
			}
		}
	}

	return tags
}

func (ws *WavefrontSynthesizer) generateTagValue(tagKey string) string {
	// Try tag-specific sampler first
	if sampler, ok := ws.tagSamplers[tagKey]; ok {
		return sampler.Sample(ws.rng)
	}

	// Try string pattern sampler
	if sampler, ok := ws.stringPatterns[tagKey]; ok {
		return sampler.Generate(ws.rng)
	}

	// Generate default value based on tag key
	switch {
	case strings.Contains(strings.ToLower(tagKey), "env"):
		envs := []string{"prod", "staging", "dev", "test"}
		return envs[ws.rng.Intn(len(envs))]
	case strings.Contains(strings.ToLower(tagKey), "region"):
		regions := []string{"us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"}
		return regions[ws.rng.Intn(len(regions))]
	case strings.Contains(strings.ToLower(tagKey), "service"):
		return fmt.Sprintf("service-%d", ws.rng.Intn(100))
	case strings.Contains(strings.ToLower(tagKey), "version"):
		return fmt.Sprintf("v%d.%d.%d", ws.rng.Intn(10), ws.rng.Intn(20), ws.rng.Intn(100))
	default:
		return fmt.Sprintf("value-%d", ws.rng.Intn(1000))
	}
}

func (ws *WavefrontSynthesizer) GetCurrentIntensity(currentTime time.Time) float64 {
	if len(ws.intensityCurve) == 0 {
		return 1.0
	}

	// Calculate minutes since start
	minutes := int(currentTime.Sub(ws.startTime).Minutes()) % 1440 // 24-hour cycle
	if minutes < 0 {
		minutes = 0
	} else if minutes >= len(ws.intensityCurve) {
		minutes = len(ws.intensityCurve) - 1
	}

	return ws.intensityCurve[minutes]
}

func (ws *WavefrontSynthesizer) escapeMetricName(name string) string {
	// Metric names can contain alphanumeric, dots, hyphens, underscores
	// If it contains other characters, it should be quoted
	validPattern := regexp.MustCompile(`^[a-zA-Z0-9._-]+$`)
	if validPattern.MatchString(name) {
		return name
	}

	// Quote and escape
	escaped := strings.ReplaceAll(name, `"`, `\"`)
	escaped = strings.ReplaceAll(escaped, `\`, `\\`)
	return `"` + escaped + `"`
}

func (ws *WavefrontSynthesizer) escapeTagValue(value string) string {
	// Tag values need quoting if they contain spaces or special characters
	if strings.ContainsAny(value, ` "=`) {
		escaped := strings.ReplaceAll(value, `"`, `\"`)
		escaped = strings.ReplaceAll(escaped, `\`, `\\`)
		return `"` + escaped + `"`
	}
	return value
}

func (ws *WavefrontSynthesizer) formatValue(value float64) string {
	// Format value appropriately
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return "0"
	}

	// Use appropriate precision
	if math.Abs(value) < 0.001 {
		return fmt.Sprintf("%.6f", value)
	} else if math.Abs(value) < 1 {
		return fmt.Sprintf("%.3f", value)
	} else if math.Abs(value) < 1000 {
		return fmt.Sprintf("%.1f", value)
	} else {
		return fmt.Sprintf("%.0f", value)
	}
}

// SynthesizeSpan generates a span line (if recipe supports spans)
func (ws *WavefrontSynthesizer) SynthesizeSpan(currentTime time.Time, multiplier float64) (string, error) {
	schema, ok := ws.recipe.Schema["schema"].(map[string]interface{})
	if !ok {
		return "", fmt.Errorf("invalid schema format")
	}

	schemaType, _ := schema["type"].(string)
	if schemaType != "span" {
		return "", fmt.Errorf("recipe is not for spans")
	}

	// Generate span: <operation> source=<source> <spanTags> <start_ms> <duration_ms>
	operation := ws.recipe.MetricName
	source := ws.generateSource()
	
	// Generate span tags (similar to metric tags)
	tags := ws.generateTags()
	
	// Generate timing
	startMs := currentTime.UnixMilli()
	durationMs := int64(ws.rng.ExpFloat64()*1000) + 1 // 1+ ms, exponential distribution

	var line strings.Builder
	line.WriteString(operation)
	line.WriteString(" source=")
	line.WriteString(ws.escapeTagValue(source))

	for key, val := range tags {
		line.WriteString(" ")
		line.WriteString(key)
		line.WriteString("=")
		line.WriteString(ws.escapeTagValue(val))
	}

	line.WriteString(" ")
	line.WriteString(strconv.FormatInt(startMs, 10))
	line.WriteString(" ")
	line.WriteString(strconv.FormatInt(durationMs, 10))

	return line.String(), nil
}

// CalculateTargetRate computes the target emission rate for current time
func (ws *WavefrontSynthesizer) CalculateTargetRate(currentTime time.Time, baseRate, multiplier, burstFactor float64) float64 {
	intensity := ws.GetCurrentIntensity(currentTime)
	
	// Apply burst factor (Hawkes-like process simulation)
	if burstFactor > 1.0 && ws.rng.Float64() < 0.1 { // 10% chance of burst
		intensity *= (1.0 + (burstFactor-1.0)*ws.rng.Float64())
	}

	return baseRate * intensity * multiplier
}

// InjectSchemaDrift adds probabilistic schema evolution
func (ws *WavefrontSynthesizer) InjectSchemaDrift(tags map[string]string, driftRate float64) map[string]string {
	if driftRate <= 0 || ws.rng.Float64() >= driftRate {
		return tags
	}

	// Add a new tag occasionally
	if ws.rng.Float64() < 0.5 {
		newKey := fmt.Sprintf("drift_tag_%d", ws.rng.Intn(10))
		tags[newKey] = fmt.Sprintf("value_%d", ws.rng.Intn(100))
	}

	// Modify existing tag value occasionally
	if len(tags) > 0 && ws.rng.Float64() < 0.3 {
		var keys []string
		for k := range tags {
			keys = append(keys, k)
		}
		key := keys[ws.rng.Intn(len(keys))]
		tags[key] = fmt.Sprintf("drift_%s", tags[key])
	}

	return tags
}

// InjectErrors adds realistic error patterns
func (ws *WavefrontSynthesizer) InjectErrors(line string, errorRate float64) string {
	if errorRate <= 0 || ws.rng.Float64() >= errorRate {
		return line
	}

	// Various error injection strategies
	switch ws.rng.Intn(5) {
	case 0:
		// Malformed metric name
		return strings.Replace(line, ws.recipe.MetricName, "invalid metric name", 1)
	case 1:
		// Missing source
		return regexp.MustCompile(`source=[^\s]+`).ReplaceAllString(line, "")
	case 2:
		// Invalid value
		return regexp.MustCompile(`\s-?\d+\.?\d*\s`).ReplaceAllString(line, " NaN ")
	case 3:
		// Truncated line
		if len(line) > 10 {
			return line[:len(line)/2]
		}
	case 4:
		// Invalid tag format
		return strings.Replace(line, "=", "==", 1)
	}

	return line
}