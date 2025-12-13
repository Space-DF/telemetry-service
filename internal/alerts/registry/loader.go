package registry

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// processorConfig mirrors the YAML structure for a processor entry.
type processorConfig struct {
	Category        string            `yaml:"category"`
	ValueKey        string            `yaml:"value_key"`
	Unit            string            `yaml:"unit"`
	StatePredicate  string            `yaml:"state_predicate"`
	DefaultWarning  float64           `yaml:"default_warning"`
	DefaultCritical float64           `yaml:"default_critical"`
	Messages        map[string]string `yaml:"messages"`
}

type processorsConfig struct {
	Processors []processorConfig `yaml:"processors"`
}

// LoadFromConfig loads processors from a YAML file and returns them keyed by lowercased category.
func LoadFromConfig(path string) (map[string]Processor, error) {
	data, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return nil, fmt.Errorf("read processors config: %w", err)
	}

	var cfg processorsConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse processors config: %w", err)
	}

	result := make(map[string]Processor, len(cfg.Processors))
	for _, p := range cfg.Processors {
		category := strings.TrimSpace(p.Category)
		if category == "" {
			return nil, fmt.Errorf("processor category is required")
		}

		valueKey := strings.TrimSpace(p.ValueKey)
		if valueKey == "" {
			valueKey = "value"
		}

		result[strings.ToLower(category)] = &GenericNumericProcessor{
			CategoryValue:   category,
			ValueKeyValue:   valueKey,
			UnitValue:       p.Unit,
			StatePred:       p.StatePredicate,
			DefaultWarn:     p.DefaultWarning,
			DefaultCritical: p.DefaultCritical,
			Messages:        p.Messages,
		}
	}

	return result, nil
}

// RegisterFromConfig merges processors from YAML into the global registry, overriding existing categories.
func RegisterFromConfig(path string) error {
	processors, err := LoadFromConfig(path)
	if err != nil {
		return err
	}

	globalRegistry.mu.Lock()
	defer globalRegistry.mu.Unlock()

	for category, processor := range processors {
		globalRegistry.processors[category] = processor
	}
	return nil
}
