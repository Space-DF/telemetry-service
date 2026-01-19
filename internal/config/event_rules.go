package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// EventRuleConfig represents a single event rule configuration
type EventRuleConfig struct {
	RuleKey         string `yaml:"rule_key"`
	EntityIDPattern string `yaml:"entity_id_pattern"`
	Operator        string `yaml:"operator"`
	Operand         string `yaml:"operand"`
	EventType       string `yaml:"event_type"`
	EventLevel      string `yaml:"event_level"`
	Description     string `yaml:"description"`
	Status          string `yaml:"status"`
	IsActive        bool   `yaml:"is_active"`
}

// DeviceModelRules represents event rules for a specific device model
type DeviceModelRules struct {
	DeviceModel   string             `yaml:"device_model"`
	DeviceModelID string             `yaml:"device_model_id"`
	DisplayName   string             `yaml:"display_name"`
	Rules         []EventRuleConfig  `yaml:"rules"`
}

// EventRulesConfig represents the aggregated event rules configuration
type EventRulesConfig struct {
	DeviceModels []DeviceModelRules `yaml:"device_models"`
}

// LoadEventRulesConfig loads event rules from a YAML file
func LoadEventRulesConfig(path string) (*EventRulesConfig, error) {
	if path == "" {
		return nil, fmt.Errorf("event rules config path is empty")
	}

	// Validate the path is within allowed directories (security: prevent path traversal)
	absPath, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("invalid path: %w", err)
	}
	absPath = filepath.Clean(absPath)

	if !isPathAllowed(absPath) {
		return nil, fmt.Errorf("path traversal detected: file must be within configs directory: %s", path)
	}

	data, err := os.ReadFile(absPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read event rules config file: %w", err)
	}

	var cfg EventRulesConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse event rules config: %w", err)
	}

	return &cfg, nil
}

// LoadEventRulesFromDir loads all event rule YAML files from a directory
// Each file should contain a single device model's rules
func LoadEventRulesFromDir(dir string) (*EventRulesConfig, error) {
	if dir == "" {
		return nil, fmt.Errorf("event rules directory is empty")
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to read event rules directory: %w", err)
	}

	var cfg EventRulesConfig

	for _, entry := range entries {
		// Skip directories and non-YAML files
		if entry.IsDir() {
			continue
		}

		// Check for .yaml or .yml extension
		name := entry.Name()
		ext := filepath.Ext(name)
		if ext != ".yaml" && ext != ".yml" {
			continue
		}

		// Load device model rules from file
		path := filepath.Join(dir, name)
		dmRules, err := loadDeviceModelRules(path)
		if err != nil {
			// Log warning but continue loading other files
			fmt.Printf("Warning: failed to load %s: %v\n", name, err)
			continue
		}

		cfg.DeviceModels = append(cfg.DeviceModels, *dmRules)
	}

	if len(cfg.DeviceModels) == 0 {
		return nil, fmt.Errorf("no valid event rules found in directory: %s", dir)
	}

	return &cfg, nil
}

// loadDeviceModelRules loads a single device model's rules from a YAML file
func loadDeviceModelRules(path string) (*DeviceModelRules, error) {
	// Validate the path is within allowed directories (security: prevent path traversal)
	absPath, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("invalid path: %w", err)
	}
	absPath = filepath.Clean(absPath)

	// Check if file is within an allowed directory (configs/event_rules or configs)
	// This prevents directory traversal attacks
	if !isPathAllowed(absPath) {
		return nil, fmt.Errorf("path traversal detected: file must be within configs directory: %s", path)
	}

	data, err := os.ReadFile(absPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	var dm DeviceModelRules
	if err := yaml.Unmarshal(data, &dm); err != nil {
		return nil, fmt.Errorf("failed to parse device model rules: %w", err)
	}

	return &dm, nil
}

// isPathAllowed checks if a path is within the configs directory
func isPathAllowed(path string) bool {
	absPath := filepath.Clean(path)
	// Check for common allowed prefixes
	allowedPrefixes := []string{
		"configs/event_rules",
		"configs" + string(filepath.Separator) + "event_rules",
	}

	for _, prefix := range allowedPrefixes {
		allowedPath, _ := filepath.Abs(prefix)
		if strings.HasPrefix(absPath, allowedPath) {
			return true
		}
	}
	return false
}

// GetRulesForDeviceModel returns all rules for a specific device model
func (c *EventRulesConfig) GetRulesForDeviceModel(deviceModel string) []EventRuleConfig {
	for _, dm := range c.DeviceModels {
		if dm.DeviceModel == deviceModel {
			return dm.Rules
		}
	}
	return nil
}

// GetDeviceModelRules returns the DeviceModelRules for a specific device model
func (c *EventRulesConfig) GetDeviceModelRules(deviceModel string) *DeviceModelRules {
	for i := range c.DeviceModels {
		if c.DeviceModels[i].DeviceModel == deviceModel {
			return &c.DeviceModels[i]
		}
	}
	return nil
}

// ToRawMap converts the EventRulesConfig to a map[string]interface{} for use with SeedDefaultEventRules
func (c *EventRulesConfig) ToRawMap() map[string]interface{} {
	deviceModels := make([]interface{}, 0, len(c.DeviceModels))
	for _, dm := range c.DeviceModels {
		rules := make([]interface{}, 0, len(dm.Rules))
		for _, r := range dm.Rules {
			rules = append(rules, map[string]interface{}{
				"rule_key":           r.RuleKey,
				"entity_id_pattern":  r.EntityIDPattern,
				"operator":          r.Operator,
				"operand":           r.Operand,
				"event_type":        r.EventType,
				"event_level":       r.EventLevel,
				"description":       r.Description,
				"status":            r.Status,
				"is_active":         r.IsActive,
			})
		}
		deviceModels = append(deviceModels, map[string]interface{}{
			"device_model":   dm.DeviceModel,
			"device_model_id": dm.DeviceModelID,
			"display_name":   dm.DisplayName,
			"rules":          rules,
		})
	}
	return map[string]interface{}{
		"device_models": deviceModels,
	}
}
