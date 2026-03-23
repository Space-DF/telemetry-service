package registry

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

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

	r.logger.Info("Loaded rule groups",
		zap.String("device_id", deviceID),
		zap.Int("rule_key_groups", len(rulesByKey)))

	// Evaluate custom automation rules if they exist.
	if len(rulesByKey) > 0 {
		// Split rules into definition-based and geofence-based
		seenIDs := make(map[string]bool)
		var definitionRules, geofenceRules []evaluator.EventRuleForEvaluation

		for _, rules := range rulesByKey {
			for _, rule := range rules {
				uniqueKey := rule.AutomationID
				if rule.GeofenceID != nil && *rule.GeofenceID != "" {
					uniqueKey = *rule.GeofenceID
				}
				if uniqueKey == "" {
					uniqueKey = rule.EventRuleID
				}
				if seenIDs[uniqueKey] {
					continue
				}
				seenIDs[uniqueKey] = true

				if rule.GeofenceID != nil && *rule.GeofenceID != "" {
					geofenceRules = append(geofenceRules, rule)
				} else {
					definitionRules = append(definitionRules, rule)
				}
			}
		}

		r.logger.Info("Split rules",
			zap.String("device_id", deviceID),
			zap.Int("automation_rules", len(definitionRules)),
			zap.Int("geofence_rules", len(geofenceRules)))

		// Evaluate definition rules against unified context from ALL entities
		for _, rule := range definitionRules {
			matched := r.evaluator.EvaluateRuleDBWithEntities(rule, deviceID, entities, map[string]interface{}{}, map[string]interface{}{})
			ruleKey := ""
			if rule.RuleKey != nil {
				ruleKey = *rule.RuleKey
			}
			r.logger.Info("Automation rule evaluated",
				zap.String("device_id", deviceID),
				zap.String("rule_key", ruleKey),
				zap.String("event_rule_id", rule.EventRuleID),
				zap.Bool("matched", matched != nil))
			if matched != nil {
				matchedEvents = append(matchedEvents, *matched)
				if ruleKey != "" {
					matchedRuleKeys[ruleKey] = true
				}
			}
		}

		// Evaluate geofence rules
		if len(geofenceRules) > 0 {
			if lat, lon, locationStateID, hasLocation := extractLocation(entities); hasLocation {
				r.logger.Info("Evaluating geofence rules with location",
					zap.String("device_id", deviceID),
					zap.Float64("lat", lat),
					zap.Float64("lon", lon),
					zap.Int("geofence_rule_count", len(geofenceRules)))

				// Check all geofences and classify results
				type geofenceResult struct {
					rule     evaluator.EventRuleForEvaluation
					isInside bool
					typeZone string
				}

				var results []geofenceResult
				isInsideSafeZone := false

				for _, rule := range geofenceRules {
					if rule.IsActive != nil && !*rule.IsActive {
						continue
					}

					isInside, typeZone, err := r.db.IsPointInGeofence(ctx, *rule.GeofenceID, lat, lon)
					if err != nil {
						r.logger.Warn("IsPointInGeofence failed",
							zap.String("geofence_id", *rule.GeofenceID),
							zap.Error(err))
						continue
					}

					r.logger.Info("Geofence check",
						zap.String("device_id", deviceID),
						zap.String("geofence_id", *rule.GeofenceID),
						zap.String("type_zone", typeZone),
						zap.Bool("is_inside", isInside))

					// If device is inside any safe zone → device is safe, no events needed
					if typeZone == "safe" && isInside {
						isInsideSafeZone = true
						break
					}

					results = append(results, geofenceResult{rule: rule, isInside: isInside, typeZone: typeZone})
				}

				// If device is in a safe zone, skip all geofence events
				if isInsideSafeZone {
					r.logger.Info("Device is inside a safe zone, skipping geofence events",
						zap.String("device_id", deviceID))
				} else {
					// Find the highest-priority triggered geofence (device must be inside)
					var bestMatch *models.MatchedEvent
					bestPriority := -1

					for _, res := range results {
						rule := res.rule
						geofenceIDDebug := ""
						if rule.GeofenceID != nil {
							geofenceIDDebug = *rule.GeofenceID
						}

						// Evaluate additional definition conditions first (for distance-based triggering)
						definitionMatched := false
						hasDefinition := rule.Definition != nil && *rule.Definition != ""

						if hasDefinition {
							extraCtx := map[string]interface{}{}
							distKm, distErr := r.db.DistanceToGeofenceKm(ctx, *rule.GeofenceID, lat, lon)
							if distErr == nil {
								r.logger.Info("Geofence distance calculated",
									zap.String("device_id", deviceID),
									zap.String("geofence_id", geofenceIDDebug),
									zap.Float64("distance_km", distKm))
								extraCtx["distance_from_geofence_km"] = distKm
							}
							definitionMatched = r.evaluator.EvaluateRuleDBWithEntities(rule, deviceID, entities, map[string]interface{}{}, extraCtx) != nil
							r.logger.Info("Geofence definition evaluation result",
								zap.String("device_id", deviceID),
								zap.String("geofence_id", geofenceIDDebug),
								zap.Bool("definition_matched", definitionMatched))
						}

						// Determine shouldTrigger based on zone type
						// Danger: trigger if inside OR if definition matches (e.g. distance <= threshold)
						// Safe:   trigger if NOT inside; if definition exists, also require it to match (weekday/time)
						shouldTrigger := false
						switch res.typeZone {
						case "safe":
							if hasDefinition {
								shouldTrigger = !res.isInside && !definitionMatched
							} else {
								shouldTrigger = !res.isInside
							}
						default:
							if hasDefinition {
								shouldTrigger = res.isInside || definitionMatched
							} else {
								shouldTrigger = res.isInside
							}
						}

						r.logger.Info("Geofence shouldTrigger evaluated",
							zap.String("device_id", deviceID),
							zap.String("geofence_id", geofenceIDDebug),
							zap.String("type_zone", res.typeZone),
							zap.Bool("is_inside", res.isInside),
							zap.Bool("has_definition", hasDefinition),
							zap.Bool("definition_matched", definitionMatched),
							zap.Bool("should_trigger", shouldTrigger))

						if !shouldTrigger {
							r.logger.Info("Geofence skipped - shouldTrigger is false",
								zap.String("device_id", deviceID),
								zap.String("geofence_id", geofenceIDDebug))
							continue
						}

						// Priority: danger = 2, other = 1, safe exit = 0
						priority := 0
						switch res.typeZone {
						case "danger", "restricted":
							priority = 2
						case "safe":
							priority = 0
						default:
							priority = 1
						}

						if priority > bestPriority {
							_, geofenceTitle, eventDesc := r.evaluateGeofenceTrigger(res.isInside, res.typeZone, *rule.GeofenceID)

							if rule.Description != nil && *rule.Description != "" {
								eventDesc = *rule.Description
							}

							ruleKey := ""
							if rule.RuleKey != nil {
								ruleKey = *rule.RuleKey
							}

							var eventRuleID *string
							if rule.EventRuleID != "" {
								erid := rule.EventRuleID
								eventRuleID = &erid
							}

							event := models.MatchedEvent{
								EntityID:     deviceID,
								EntityType:   "location",
								RuleKey:      ruleKey,
								EventType:    "device_event",
								EventLevel:   "automation",
								Title:        geofenceTitle,
								Description:  eventDesc,
								Value:        lat,
								Threshold:    lon,
								Operator:     "geofence:" + res.typeZone,
								Timestamp:    time.Now().UnixMilli(),
								EventRuleID:  eventRuleID,
								AutomationID: nil,
								GeofenceID:   rule.GeofenceID,
								StateID:      locationStateID,
								Location:     &models.Location{Latitude: lat, Longitude: lon},
							}
							bestMatch = &event
							bestPriority = priority
						}
					}

					if bestMatch != nil {
						matchedEvents = append(matchedEvents, *bestMatch)
						if bestMatch.RuleKey != "" {
							matchedRuleKeys[bestMatch.RuleKey] = true
						}
					}
				}
			}
		}
	}

	r.logger.Info("Evaluate done with custom rules",
		zap.String("device_id", deviceID),
		zap.Int("matched_events", len(matchedEvents)))

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

// evaluateGeofenceTrigger determines if geofence should trigger based on zone type.
// Returns: triggered, title, description
func (r *RuleRegistry) evaluateGeofenceTrigger(isInside bool, typeZone, geofenceID string) (bool, string, string) {
	switch typeZone {
	case "safe":
		title := "Device exited Safe Zone"
		return !isInside, title, fmt.Sprintf("%s (geofence %s)", title, geofenceID)
	case "danger":
		title := "Device entered Danger Zone"
		return isInside, title, fmt.Sprintf("%s (geofence %s)", title, geofenceID)
	default:
		var title string
		if isInside {
			title = fmt.Sprintf("Device entered %s zone", typeZone)
		} else {
			title = fmt.Sprintf("Device exited %s zone", typeZone)
		}
		return isInside, title, fmt.Sprintf("%s (geofence %s)", title, geofenceID)
	}
}

// extractLocation searches the entity list for a location-type entity and returns lat/lon/stateID.
func extractLocation(entities []models.TelemetryEntity) (lat, lon float64, stateID *string, found bool) {
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
			return latF, lonF, e.StateID, true
		}
	}
	return 0, 0, nil, false
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

// GetDefaultRules returns all loaded default rules (for Infoging/inspection)
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
