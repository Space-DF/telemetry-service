package registry

import (
	"fmt"
	"strings"
	"sync"
)

// Processor defines behavior for building alerts for a given category/device type.
type Processor interface {
	Category() string
	DefaultWarningThreshold() float64
	DefaultCriticalThreshold() float64
	Unit() string
	ValueKey() string
	StatePredicate() string
	ParseValue(raw string) (float64, error)
	DetermineLevel(value, warningThreshold, criticalThreshold float64) string
	DetermineType(value, warningThreshold, criticalThreshold float64) string
	GenerateMessage(level string, value float64) string
}

type registry struct {
	mu         sync.RWMutex
	processors map[string]Processor
}

var globalRegistry = &registry{processors: make(map[string]Processor)}

// Register adds a processor; categories are stored lowercased.
func Register(p Processor) error {
	if p == nil {
		return fmt.Errorf("processor cannot be nil")
	}

	cat := strings.ToLower(p.Category())

	globalRegistry.mu.Lock()
	defer globalRegistry.mu.Unlock()

	if _, exists := globalRegistry.processors[cat]; exists {
		return fmt.Errorf("processor for category %s already registered", cat)
	}

	globalRegistry.processors[cat] = p
	return nil
}

// Get returns a processor by category (case-insensitive).
func Get(category string) (Processor, bool) {
	globalRegistry.mu.RLock()
	defer globalRegistry.mu.RUnlock()

	p, ok := globalRegistry.processors[strings.ToLower(category)]
	return p, ok
}

// ReplaceAll swaps the registry contents with the provided processors map.
func ReplaceAll(processors map[string]Processor) {
	globalRegistry.mu.Lock()
	defer globalRegistry.mu.Unlock()
	globalRegistry.processors = copyProcessors(processors)
}

func copyProcessors(in map[string]Processor) map[string]Processor {
	out := make(map[string]Processor, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
