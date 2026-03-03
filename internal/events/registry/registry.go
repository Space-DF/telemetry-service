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
	defaultRules        map[string]*loader.DeviceModelRules
	groupedDefaultRules map[string]map[string][]loader.YAMLRule // "brand/model" → rule_key → rules
	defaultRulesMu      sync.RWMutex

	// Cache for device automation rules
	cache *DeviceRulesCache

	evaluator *evaluator.Evaluator
	db        *timescaledb.Client
	logger    *zap.Logger
}

// NewRuleRegistry creates a new rule registry
func NewRuleRegistry(db *timescaledb.Client, logger *zap.Logger) *RuleRegistry {
	r := &RuleRegistry{
		defaultRules:        make(map[string]*loader.DeviceModelRules),
		groupedDefaultRules: make(map[string]map[string][]loader.YAMLRule),
		cache:               NewDeviceRulesCache(db, logger),
		evaluator:           evaluator.NewEvaluator(logger),
		db:                  db,
		logger:              logger,
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

	// Group default rules by rule_key for O(1) lookup
	r.groupedDefaultRules = make(map[string]map[string][]loader.YAMLRule)
	for key, dm := range rules {
		grouped := make(map[string][]loader.YAMLRule)
		for _, rule := range dm.Rules {
			grouped[rule.RuleKey] = append(grouped[rule.RuleKey], rule)
		}
		r.groupedDefaultRules[key] = grouped
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

	// Try to get grouped automation rules from cache first
	rulesByKey := r.cache.GetGrouped(ctx, deviceID)

	// Evaluate custom automation rules if they exist.
	if len(rulesByKey) > 0 {
		// Split rules into definition-based and geofence-based (deduplicated)
		seenIDs := make(map[string]bool)
		var definitionRules, geofenceRules []models.EventRule

		for _, rules := range rulesByKey {
			for _, rule := range rules {
				if seenIDs[rule.EventRuleID] {
					continue
				}
				seenIDs[rule.EventRuleID] = true

				if rule.GeofenceID != nil && *rule.GeofenceID != "" {
					geofenceRules = append(geofenceRules, rule)
				} else {
					definitionRules = append(definitionRules, rule)
				}
			}
		}

		// Evaluate definition rules against unified context from ALL entities
		for _, rule := range definitionRules {
			if matched := r.evaluator.EvaluateRuleDBWithEntities(rule, deviceID, entities, nil); matched != nil {
				matchedEvents = append(matchedEvents, *matched)
				if rule.RuleKey != nil && *rule.RuleKey != "" {
					matchedRuleKeys[*rule.RuleKey] = true
				}
			}
		}

		// Evaluate geofence rules
		if len(geofenceRules) > 0 {
			if lat, lon, hasLocation := extractLocation(entities); hasLocation {
				for _, rule := range geofenceRules {
					if rule.IsActive != nil && !*rule.IsActive {
						continue
					}

					isInside, typeZone, err := r.db.IsPointInGeofence(ctx, *rule.GeofenceID, lat, lon)
					if err != nil {
						continue
					}

					// Determine trigger condition based on zone type
					geofenceTriggered, eventDesc := r.evaluateGeofenceTrigger(isInside, typeZone, *rule.GeofenceID)
					if !geofenceTriggered {
						continue
					}

					// If rule has a definition, also check those conditions
					if rule.Definition != nil && *rule.Definition != "" {
						if r.evaluator.EvaluateRuleDBWithEntities(rule, deviceID, entities, nil) == nil {
							continue // Definition conditions not met
						}
					}

					// Use rule's description if available
					if rule.Description != nil && *rule.Description != "" {
						eventDesc = *rule.Description
					}

					ruleKey := ""
					if rule.RuleKey != nil {
						ruleKey = *rule.RuleKey
					}

					matchedEvents = append(matchedEvents, models.MatchedEvent{
						EntityID:    deviceID,
						EntityType:  "location",
						RuleKey:     ruleKey,
						EventType:   "device_event",
						EventLevel:  "automation",
						Description: eventDesc,
						Value:       lat,
						Threshold:   lon,
						Operator:    "geofence:" + typeZone,
						Timestamp:   0,
						EventRuleID: &rule.EventRuleID,
					})

					if ruleKey != "" {
						matchedRuleKeys[ruleKey] = true
					}
				}
			}
		}
	}

	// Evaluate default system rules — skip rule_keys already matched by custom rules
	r.defaultRulesMu.RLock()
	key := fmt.Sprintf("%s/%s", strings.ToLower(brand), strings.ToLower(model))
	defaultRulesByKey, exists := r.groupedDefaultRules[key]
	r.defaultRulesMu.RUnlock()

	if exists {
		for _, entity := range entities {
			if entity.EntityType == "" || matchedRuleKeys[entity.EntityType] {
				continue
			}
			if rules, ok := defaultRulesByKey[entity.EntityType]; ok {
				for _, rule := range rules {
					if matched := r.evaluator.EvaluateRule(rule, deviceID, entity); matched != nil {
						matchedEvents = append(matchedEvents, *matched)
					}
				}
			}
		}
	}

	return matchedEvents
}

// evaluateGeofenceTrigger determines if geofence should trigger based on zone type
func (r *RuleRegistry) evaluateGeofenceTrigger(isInside bool, typeZone, geofenceID string) (bool, string) {
	switch typeZone {
	case "safe":
		return !isInside, fmt.Sprintf("Device left safe zone (geofence %s)", geofenceID)
	case "danger":
		return isInside, fmt.Sprintf("Device entered danger zone (geofence %s)", geofenceID)
	default:
		return isInside, fmt.Sprintf("Device is inside zone '%s' (geofence %s)", typeZone, geofenceID)
	}
}

// extractLocation searches the entity list for a location-type entity and returns lat/lon.
func extractLocation(entities []models.TelemetryEntity) (lat, lon float64, found bool) {
	for _, e := range entities {
		if strings.ToLower(e.EntityType) != "location" {
			continue
		}
		latVal, hasLat := e.Attributes["latitude"]
		lonVal, hasLon := e.Attributes["longitude"]
		if !hasLat || !hasLon {
			continue
		}
		latF, ok1 := toFloat64(latVal)
		lonF, ok2 := toFloat64(lonVal)
		if ok1 && ok2 {
			return latF, lonF, true
		}
	}
	return 0, 0, false
}

func toFloat64(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case float64:
		return val, true
	case float32:
		return float64(val), true
	case int:
		return float64(val), true
	case int64:
		return float64(val), true
	default:
		return 0, false
	}
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
