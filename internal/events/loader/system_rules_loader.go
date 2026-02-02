package loader

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"
	"gopkg.in/yaml.v3"
)

// YAMLRule represents a single event rule from YAML configuration
type YAMLRule struct {
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
	Brand       string     `yaml:"brand"`       // e.g., "rakwireless"
	Model       string     `yaml:"model"`       // e.g., "rak4630"
	ModelID     string     `yaml:"model_id"`    // Resolved from device service
	DisplayName string     `yaml:"display_name"`
	Rules       []YAMLRule `yaml:"rules"`
}

// LoadSystemDefaultRules loads YAML files from brand/model directory structure
// These are system default rules that apply to all devices of a specific brand/model
func LoadSystemDefaultRules(dir string) (map[string]*DeviceModelRules, error) {
	result := make(map[string]*DeviceModelRules)

	// Walk through the directory structure
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip directories and non-YAML files
		if info.IsDir() {
			return nil
		}

		ext := strings.ToLower(filepath.Ext(path))
		if ext != ".yaml" && ext != ".yml" {
			return nil
		}

		// Load the device model rules from this file
		rules, err := loadDeviceModelRules(path, dir)
		if err != nil {
			// Log warning but continue loading other files
			fmt.Printf("Warning: failed to load %s: %v\n", path, err)
			return nil
		}

		// Index by brand/model
		key := fmt.Sprintf("%s/%s", strings.ToLower(rules.Brand), strings.ToLower(rules.Model))
		result[key] = rules

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to walk directory: %w", err)
	}

	if len(result) == 0 {
		return nil, fmt.Errorf("no valid event rules found in directory: %s", dir)
	}

	return result, nil
}

// loadDeviceModelRules loads rules from a single YAML file
func loadDeviceModelRules(filePath, baseDir string) (*DeviceModelRules, error) {
	absBaseDir, err := filepath.Abs(baseDir)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve base directory: %w", err)
	}

	absFilePath, err := filepath.Abs(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve file path: %w", err)
	}

	// Check if the file path is within the base directory
	relPath, err := filepath.Rel(absBaseDir, absFilePath)
	if err != nil || strings.HasPrefix(relPath, "..") {
		return nil, fmt.Errorf("file path is outside base directory: %s", filePath)
	}

	// #nosec G304 -- Path is validated above to ensure it's within baseDir
	data, err := os.ReadFile(absFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	var rules DeviceModelRules
	if err := yaml.Unmarshal(data, &rules); err != nil {
		return nil, fmt.Errorf("failed to parse YAML: %w", err)
	}

	// Validate required fields
	if rules.Brand == "" {
		return nil, fmt.Errorf("missing required field: brand")
	}
	if rules.Model == "" {
		return nil, fmt.Errorf("missing required field: model")
	}

	// Set defaults
	if rules.DisplayName == "" {
		caser := cases.Title(language.English)
		rules.DisplayName = fmt.Sprintf("%s %s Rules", caser.String(rules.Brand), strings.ToUpper(rules.Model))
	}

	for i := range rules.Rules {
		if rules.Rules[i].Status == "" {
			rules.Rules[i].Status = "active"
		}
	}

	return &rules, nil
}

// GetRulesForDevice retrieves rules for a specific brand/model combination
func GetRulesForDevice(loadedRules map[string]*DeviceModelRules, brand, model string) *DeviceModelRules {
	key := fmt.Sprintf("%s/%s", strings.ToLower(brand), strings.ToLower(model))
	return loadedRules[key]
}
