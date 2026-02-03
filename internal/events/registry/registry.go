package registry

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/Space-DF/telemetry-service/internal/events/evaluator"
	"github.com/Space-DF/telemetry-service/internal/events/loader"
	"github.com/Space-DF/telemetry-service/internal/models"
	"github.com/Space-DF/telemetry-service/internal/timescaledb"
	"go.uber.org/zap"
)

// RuleRegistry manages event rules from both YAML files and database
type RuleRegistry struct {
	// Default rules from YAML (key: "brand/model" e.g., "rakwireless/rak4630")
	defaultRules map[string]*loader.DeviceModelRules
	defaultRulesMu sync.RWMutex

	// Cache for device automation rules
	cache *DeviceRulesCache

	evaluator *evaluator.Evaluator
	db        *timescaledb.Client
	logger    *zap.Logger
}

// NewRuleRegistry creates a new rule registry
func NewRuleRegistry(db *timescaledb.Client, logger *zap.Logger) *RuleRegistry {
	r := &RuleRegistry{
		defaultRules: make(map[string]*loader.DeviceModelRules),
		cache:        NewDeviceRulesCache(db, logger),
		evaluator:    evaluator.NewEvaluator(logger),
		db:          db,
		logger:      logger,
	}

	// Start background cache cleanup
	r.cache.Start()

	return r
}

// LoadDefaultRulesFromDir loads system default event rules from YAML files organized by brand/model
func (r *RuleRegistry) LoadDefaultRulesFromDir(dir string) error {
	r.defaultRulesMu.Lock()
	defer r.defaultRulesMu.Unlock()

	rules, err := loader.LoadSystemDefaultRules(dir)
	if err != nil {
		return fmt.Errorf("failed to load default rules from directory: %w", err)
	}

	r.defaultRules = rules

	// Log loaded rules
	for _, dm := range rules {
		r.logger.Info("Loaded default event rules",
			zap.String("brand", dm.Brand),
			zap.String("model", dm.Model),
			zap.Int("rule_count", len(dm.Rules)))
	}

	r.logger.Info("Default event rules loaded successfully",
		zap.Int("device_models", len(rules)))

	return nil
}

// This function is the core of the rule evaluation process, It checks if there's any custom automation event rules that
// Created by the user for the the specific device. If there are, it evaluates those first. If not, it falls back to the default system rules
func (r *RuleRegistry) Evaluate(ctx context.Context, deviceID, brand, model string, entities []models.TelemetryEntity) []models.MatchedEvent {
	var matchedEvents []models.MatchedEvent
	matchedRuleKeys := make(map[string]bool)

	if len(entities) == 0 {
		return matchedEvents
	}

	// Try to get automation rules from cache first
	customRules := r.cache.Get(ctx, deviceID)

	// Evaluate custom automation rules if they exist
	if len(customRules) > 0 {
		r.logger.Debug("Using custom automation rules for device",
			zap.String("device_id", deviceID),
			zap.Int("rule_count", len(customRules)))

		// Group rules by rule_key for O(1) lookup
		rulesByKey := r.groupRulesByKey(customRules)

		// Match entity attributes to rules by rule_key
		for _, entity := range entities {
			// Track which rule_keys we've processed for this entity
			processedKeys := make(map[string]bool)

			// Check each attribute in the entity
			if entity.Attributes != nil {
				for attrKey := range entity.Attributes {
					// Skip if we already processed a rule with this key
					if processedKeys[attrKey] {
						continue
					}
					processedKeys[attrKey] = true

					// Find rules that match this attribute key
					if rules, exists := rulesByKey[attrKey]; exists {
						for _, rule := range rules {
							if matched := r.evaluator.EvaluateRuleDB(rule, deviceID, entity); matched != nil {
								matchedEvents = append(matchedEvents, *matched)
								matchedRuleKeys[matched.RuleKey] = true
							}
						}
					}
				}
			}

			// Also check entity_type for state-based entities
			if entity.EntityType != "" {
				if rules, exists := rulesByKey[entity.EntityType]; exists {
					if !processedKeys[entity.EntityType] {
						processedKeys[entity.EntityType] = true
						for _, rule := range rules {
							if matched := r.evaluator.EvaluateRuleDB(rule, deviceID, entity); matched != nil {
								matchedEvents = append(matchedEvents, *matched)
								matchedRuleKeys[matched.RuleKey] = true
							}
						}
					}
				}
			}
		}
	}

	// Evaluate default system rules
	// Only for rule_keys that didn't match custom automation rules
	r.defaultRulesMu.RLock()
	key := fmt.Sprintf("%s/%s", strings.ToLower(brand), strings.ToLower(model))
	defaultRules, exists := r.defaultRules[key]
	r.defaultRulesMu.RUnlock()

	if exists {
		// Group default rules by rule_key
		defaultRulesByKey := make(map[string][]loader.YAMLRule)
		for _, rule := range defaultRules.Rules {
			defaultRulesByKey[rule.RuleKey] = append(defaultRulesByKey[rule.RuleKey], rule)
		}

		for _, entity := range entities {
			// Track processed keys to avoid duplicate evaluations
			processedKeys := make(map[string]bool)

			// Check entity attributes
			if entity.Attributes != nil {
				for attrKey := range entity.Attributes {
					// Skip if custom automation rule already matched for this rule_key
					if matchedRuleKeys[attrKey] {
						continue
					}
					processedKeys[attrKey] = true

					// Find default rules for this attribute
					if rules, exists := defaultRulesByKey[attrKey]; exists {
						for _, rule := range rules {
							if matched := r.evaluator.EvaluateRule(rule, deviceID, entity); matched != nil {
								matchedEvents = append(matchedEvents, *matched)
							}
						}
					}
				}
			}

			// Check entity_type for state-based entities
			if entity.EntityType != "" {
				// Skip if custom automation rule already matched for this rule_key
				if !matchedRuleKeys[entity.EntityType] {
					if rules, exists := defaultRulesByKey[entity.EntityType]; exists {
						for _, rule := range rules {
							if matched := r.evaluator.EvaluateRule(rule, deviceID, entity); matched != nil {
								matchedEvents = append(matchedEvents, *matched)
							}
						}
					}
				}
			}
		}
	}

	return matchedEvents
}

// groupRulesByKey groups database rules by rule_key for efficient O(1) lookup
func (r *RuleRegistry) groupRulesByKey(rules []models.EventRule) map[string][]models.EventRule {
	rulesByKey := make(map[string][]models.EventRule)
	for _, rule := range rules {
		if rule.RuleKey != nil && *rule.RuleKey != "" {
			rulesByKey[*rule.RuleKey] = append(rulesByKey[*rule.RuleKey], rule)
		}
	}
	return rulesByKey
}

// GetDefaultRules returns all loaded default rules (for debugging/inspection)
func (r *RuleRegistry) GetDefaultRules() map[string]*loader.DeviceModelRules {
	r.defaultRulesMu.RLock()
	defer r.defaultRulesMu.RUnlock()

	// Return a copy to avoid race conditions
	result := make(map[string]*loader.DeviceModelRules, len(r.defaultRules))
	for k, v := range r.defaultRules {
		result[k] = v
	}
	return result
}

// ReloadDefaultRules reloads default rules from the configured directory
func (r *RuleRegistry) ReloadDefaultRules(dir string) error {
	r.logger.Info("Reloading default event rules", zap.String("dir", dir))
	return r.LoadDefaultRulesFromDir(dir)
}

// InvalidateDeviceCache removes cached rules for a specific device
// Call this when automation rules are created, updated, or deleted for a device
func (r *RuleRegistry) InvalidateDeviceCache(deviceID string) {
	r.cache.Invalidate(deviceID)
}

// InvalidateAllDeviceCache clears the entire device rules cache
func (r *RuleRegistry) InvalidateAllDeviceCache() {
	r.cache.InvalidateAll()
}

// GetCacheMetrics returns cache performance metrics
func (r *RuleRegistry) GetCacheMetrics() map[string]interface{} {
	return r.cache.GetMetrics()
}

// Stop stops the registry and cleanup goroutines
func (r *RuleRegistry) Stop() {
	r.cache.Stop()
}
