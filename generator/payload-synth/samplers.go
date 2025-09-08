package payloadsynth

import (
	"fmt"
	"math"
	"math/rand"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// WeightedItem represents an item with an associated weight for sampling
type WeightedItem struct {
	Value  string
	Weight float64
}

// CategoricalSampler samples from a weighted categorical distribution
type CategoricalSampler struct {
	items       []WeightedItem
	cumulativeWeights []float64
	totalWeight float64
}

// NewCategoricalSampler creates a new categorical sampler
func NewCategoricalSampler(items []WeightedItem) *CategoricalSampler {
	if len(items) == 0 {
		return &CategoricalSampler{}
	}

	sampler := &CategoricalSampler{
		items: make([]WeightedItem, len(items)),
		cumulativeWeights: make([]float64, len(items)),
	}

	copy(sampler.items, items)

	// Calculate cumulative weights
	cumulative := 0.0
	for i, item := range sampler.items {
		cumulative += item.Weight
		sampler.cumulativeWeights[i] = cumulative
	}
	sampler.totalWeight = cumulative

	return sampler
}

// Sample returns a random value according to the weighted distribution
func (cs *CategoricalSampler) Sample(rng *rand.Rand) string {
	if len(cs.items) == 0 {
		return ""
	}

	if cs.totalWeight <= 0 {
		return cs.items[rng.Intn(len(cs.items))].Value
	}

	target := rng.Float64() * cs.totalWeight
	
	// Binary search for the target weight
	idx := sort.Search(len(cs.cumulativeWeights), func(i int) bool {
		return cs.cumulativeWeights[i] >= target
	})

	if idx >= len(cs.items) {
		idx = len(cs.items) - 1
	}

	return cs.items[idx].Value
}

// NumericSampler samples from a numeric distribution
type NumericSampler struct {
	quantiles []float64
	sampler   func(*rand.Rand) float64
}

// NewQuantileSampler creates a sampler based on quantiles
func NewQuantileSampler(quantiles []float64) *NumericSampler {
	if len(quantiles) < 3 {
		// Default to normal distribution if insufficient data
		return &NumericSampler{
			quantiles: []float64{0, 25, 50, 75, 100},
			sampler: func(rng *rand.Rand) float64 {
				return rng.NormFloat64()*10 + 50
			},
		}
	}

	sort.Float64s(quantiles)
	
	return &NumericSampler{
		quantiles: quantiles,
		sampler: func(rng *rand.Rand) float64 {
			// Interpolate between quantiles
			p := rng.Float64()
			return interpolateQuantile(quantiles, p)
		},
	}
}

// NewLogNormalSampler creates a log-normal distribution sampler
func NewLogNormalSampler(mu, sigma float64) *NumericSampler {
	return &NumericSampler{
		sampler: func(rng *rand.Rand) float64 {
			return math.Exp(rng.NormFloat64()*sigma + mu)
		},
	}
}

// NewExponentialSampler creates an exponential distribution sampler
func NewExponentialSampler(lambda float64) *NumericSampler {
	return &NumericSampler{
		sampler: func(rng *rand.Rand) float64 {
			return rng.ExpFloat64() / lambda
		},
	}
}

// Sample returns a random value from the numeric distribution
func (ns *NumericSampler) Sample(rng *rand.Rand) float64 {
	return ns.sampler(rng)
}

func interpolateQuantile(quantiles []float64, p float64) float64 {
	if p <= 0 {
		return quantiles[0]
	}
	if p >= 1 {
		return quantiles[len(quantiles)-1]
	}

	// Find the interval
	n := len(quantiles) - 1
	pos := p * float64(n)
	idx := int(pos)
	
	if idx >= n {
		return quantiles[n]
	}

	// Linear interpolation
	frac := pos - float64(idx)
	return quantiles[idx] + frac*(quantiles[idx+1]-quantiles[idx])
}

// WeightedPattern represents a string pattern with weight
type WeightedPattern struct {
	Pattern string
	Weight  float64
}

// StringPatternSampler generates strings based on regex-like patterns
type StringPatternSampler struct {
	patterns      []WeightedPattern
	cumulativeWeights []float64
	totalWeight   float64
}

// NewStringPatternSampler creates a new string pattern sampler
func NewStringPatternSampler(patterns []WeightedPattern) *StringPatternSampler {
	if len(patterns) == 0 {
		// Default pattern
		patterns = []WeightedPattern{
			{Pattern: "default-[a-z]{3}-\\d{2}", Weight: 1.0},
		}
	}

	sampler := &StringPatternSampler{
		patterns: make([]WeightedPattern, len(patterns)),
		cumulativeWeights: make([]float64, len(patterns)),
	}

	copy(sampler.patterns, patterns)

	// Calculate cumulative weights
	cumulative := 0.0
	for i, pattern := range sampler.patterns {
		cumulative += pattern.Weight
		sampler.cumulativeWeights[i] = cumulative
	}
	sampler.totalWeight = cumulative

	return sampler
}

// Generate creates a string matching one of the patterns
func (sps *StringPatternSampler) Generate(rng *rand.Rand) string {
	if len(sps.patterns) == 0 {
		return "default-string"
	}

	// Select pattern
	target := rng.Float64() * sps.totalWeight
	idx := sort.Search(len(sps.cumulativeWeights), func(i int) bool {
		return sps.cumulativeWeights[i] >= target
	})

	if idx >= len(sps.patterns) {
		idx = len(sps.patterns) - 1
	}

	pattern := sps.patterns[idx].Pattern
	return sps.expandPattern(pattern, rng)
}

func (sps *StringPatternSampler) expandPattern(pattern string, rng *rand.Rand) string {
	// Simple pattern expansion (can be made more sophisticated)
	result := pattern

	// Replace common patterns
	result = sps.replacePattern(result, `\\d\+`, func() string {
		length := 1 + rng.Intn(4) // 1-4 digits
		return sps.generateDigits(rng, length)
	})

	result = sps.replacePattern(result, `\\d\{(\d+)\}`, func(matches ...string) string {
		if len(matches) > 1 {
			if length, err := strconv.Atoi(matches[1]); err == nil {
				return sps.generateDigits(rng, length)
			}
		}
		return "123"
	})

	result = sps.replacePattern(result, `\[a-z\]\+`, func() string {
		length := 3 + rng.Intn(5) // 3-7 characters
		return sps.generateLowercase(rng, length)
	})

	result = sps.replacePattern(result, `\[a-z\]\{(\d+)\}`, func(matches ...string) string {
		if len(matches) > 1 {
			if length, err := strconv.Atoi(matches[1]); err == nil {
				return sps.generateLowercase(rng, length)
			}
		}
		return "abc"
	})

	result = sps.replacePattern(result, `\[A-Z\]\+`, func() string {
		length := 3 + rng.Intn(5) // 3-7 characters
		return sps.generateUppercase(rng, length)
	})

	result = sps.replacePattern(result, `\[A-Z\]\{(\d+)\}`, func(matches ...string) string {
		if len(matches) > 1 {
			if length, err := strconv.Atoi(matches[1]); err == nil {
				return sps.generateUppercase(rng, length)
			}
		}
		return "ABC"
	})

	result = sps.replacePattern(result, `\[a-zA-Z0-9\]\+`, func() string {
		length := 5 + rng.Intn(10) // 5-14 characters
		return sps.generateAlphanumeric(rng, length)
	})

	return result
}

func (sps *StringPatternSampler) replacePattern(input, pattern string, replacer interface{}) string {
	re := regexp.MustCompile(pattern)
	
	switch r := replacer.(type) {
	case func() string:
		return re.ReplaceAllStringFunc(input, func(string) string {
			return r()
		})
	case func(...string) string:
		return re.ReplaceAllStringFunc(input, func(match string) string {
			matches := re.FindStringSubmatch(match)
			return r(matches...)
		})
	default:
		return input
	}
}

func (sps *StringPatternSampler) generateDigits(rng *rand.Rand, length int) string {
	var result strings.Builder
	for i := 0; i < length; i++ {
		result.WriteString(strconv.Itoa(rng.Intn(10)))
	}
	return result.String()
}

func (sps *StringPatternSampler) generateLowercase(rng *rand.Rand, length int) string {
	const chars = "abcdefghijklmnopqrstuvwxyz"
	var result strings.Builder
	for i := 0; i < length; i++ {
		result.WriteByte(chars[rng.Intn(len(chars))])
	}
	return result.String()
}

func (sps *StringPatternSampler) generateUppercase(rng *rand.Rand, length int) string {
	const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	var result strings.Builder
	for i := 0; i < length; i++ {
		result.WriteByte(chars[rng.Intn(len(chars))])
	}
	return result.String()
}

func (sps *StringPatternSampler) generateAlphanumeric(rng *rand.Rand, length int) string {
	const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	var result strings.Builder
	for i := 0; i < length; i++ {
		result.WriteByte(chars[rng.Intn(len(chars))])
	}
	return result.String()
}

// TimeSampler generates realistic timestamp distributions
type TimeSampler struct {
	baseTime   int64
	pattern    string // "uniform", "poisson", "bursty"
	intensity  []float64
	burstiness float64
}

// NewTimeSampler creates a time-based sampler
func NewTimeSampler(baseTime int64, pattern string, intensity []float64) *TimeSampler {
	return &TimeSampler{
		baseTime:  baseTime,
		pattern:   pattern,
		intensity: intensity,
		burstiness: 1.0,
	}
}

// SampleInterval returns the next time interval based on the pattern
func (ts *TimeSampler) SampleInterval(rng *rand.Rand, currentMinute int) float64 {
	baseInterval := 1.0 // seconds
	
	// Apply intensity curve
	if len(ts.intensity) > 0 {
		idx := currentMinute % len(ts.intensity)
		baseInterval /= ts.intensity[idx]
	}

	switch ts.pattern {
	case "poisson":
		return rng.ExpFloat64() * baseInterval
	case "bursty":
		if rng.Float64() < 0.1 { // 10% chance of burst
			return baseInterval / (1.0 + ts.burstiness*rng.Float64())
		}
		return rng.ExpFloat64() * baseInterval * 2.0
	default: // uniform
		return baseInterval * (0.5 + rng.Float64())
	}
}

// CooccurrenceSampler samples correlated tag combinations
type CooccurrenceSampler struct {
	combinations []TagCombination
	cumulativeWeights []float64
	totalWeight  float64
}

type TagCombination struct {
	Tags   map[string]string
	Weight float64
}

// NewCooccurrenceSampler creates a sampler for correlated tag combinations
func NewCooccurrenceSampler(combinations []TagCombination) *CooccurrenceSampler {
	if len(combinations) == 0 {
		return &CooccurrenceSampler{}
	}

	sampler := &CooccurrenceSampler{
		combinations: make([]TagCombination, len(combinations)),
		cumulativeWeights: make([]float64, len(combinations)),
	}

	copy(sampler.combinations, combinations)

	// Calculate cumulative weights
	cumulative := 0.0
	for i, combo := range sampler.combinations {
		cumulative += combo.Weight
		sampler.cumulativeWeights[i] = cumulative
	}
	sampler.totalWeight = cumulative

	return sampler
}

// Sample returns a correlated tag combination
func (cs *CooccurrenceSampler) Sample(rng *rand.Rand) map[string]string {
	if len(cs.combinations) == 0 {
		return make(map[string]string)
	}

	target := rng.Float64() * cs.totalWeight
	idx := sort.Search(len(cs.cumulativeWeights), func(i int) bool {
		return cs.cumulativeWeights[i] >= target
	})

	if idx >= len(cs.combinations) {
		idx = len(cs.combinations) - 1
	}

	// Copy the tag combination
	result := make(map[string]string)
	for k, v := range cs.combinations[idx].Tags {
		result[k] = v
	}

	return result
}

// EntitySampler manages per-entity (e.g., per-source) emission characteristics
type EntitySampler struct {
	entities     []string
	rates        []float64
	currentIndex int
}

// NewEntitySampler creates a sampler that rotates through entities with different rates
func NewEntitySampler(entities []string, rates []float64) *EntitySampler {
	if len(entities) != len(rates) {
		// Pad or truncate to match
		minLen := len(entities)
		if len(rates) < minLen {
			minLen = len(rates)
		}
		entities = entities[:minLen]
		rates = rates[:minLen]
	}

	return &EntitySampler{
		entities: entities,
		rates:    rates,
	}
}

// SampleEntity returns the next entity and its emission rate
func (es *EntitySampler) SampleEntity(rng *rand.Rand) (string, float64) {
	if len(es.entities) == 0 {
		return fmt.Sprintf("entity-%d", rng.Intn(100)), 1.0
	}

	// Weighted selection based on rates
	totalRate := 0.0
	for _, rate := range es.rates {
		totalRate += rate
	}

	target := rng.Float64() * totalRate
	cumulative := 0.0
	
	for i, rate := range es.rates {
		cumulative += rate
		if cumulative >= target {
			return es.entities[i], rate
		}
	}

	// Fallback
	idx := rng.Intn(len(es.entities))
	return es.entities[idx], es.rates[idx]
}