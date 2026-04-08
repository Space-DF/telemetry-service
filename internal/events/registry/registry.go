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
	"github.com/google/uuid"
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

// ruleSplit contains rules split by type
type ruleSplit struct {
	definitionRules []evaluator.EventRuleForEvaluation
	geofenceRules   []evaluator.EventRuleForEvaluation
}

// MatchAutomationEvents evaluates automation rules against telemetry entities and returns matched events.
// It checks for custom automation event rules created for the specific device. If none exist, it falls back to default system rules.
func (r *RuleRegistry) MatchAutomationEvents(ctx context.Context, deviceID, brand, model string, entities []models.TelemetryEntity) []models.MatchedEvent {
	var matchedEvents []models.MatchedEvent
	matchedRuleKeys := make(map[string]bool)

	if len(entities) == 0 {
		return matchedEvents
	}

	// Try to get grouped automation rules from cache first
	rulesByKey := r.cache.LoadAutomationRulesForDevice(ctx, deviceID)

	r.logger.Info("Loaded rule groups",
		zap.String("device_id", deviceID),
		zap.Int("rule_key_groups", len(rulesByKey)))

	// Evaluate custom automation rules if they exist.
	if len(rulesByKey) > 0 {
		split := r.splitRulesByType(rulesByKey)

		r.logger.Info("Split rules",
			zap.String("device_id", deviceID),
			zap.Int("automation_rules", len(split.definitionRules)),
			zap.Int("geofence_rules", len(split.geofenceRules)))

		// Evaluate definition rules
		matchedEvents = append(matchedEvents, r.evaluateDefinitionRules(split.definitionRules, deviceID, entities, matchedRuleKeys)...)

		// Evaluate geofence rules
		if len(split.geofenceRules) > 0 {
			geofenceEvents := r.evaluateGeofenceRules(ctx, split.geofenceRules, deviceID, entities, matchedRuleKeys)
			matchedEvents = append(matchedEvents, geofenceEvents...)
		}
	}

	r.logger.Info("Evaluate done with custom rules",
		zap.String("device_id", deviceID),
		zap.Int("matched_events", len(matchedEvents)))

	// Evaluate default system rules — skip rule_keys already matched by custom rules
	defaultEvents := r.evaluateDefaultRules(brand, model, entities, matchedRuleKeys, deviceID)
	matchedEvents = append(matchedEvents, defaultEvents...)

	return matchedEvents
}

// splitRulesByType splits automation rules into definition-based and geofence-based rules
func (r *RuleRegistry) splitRulesByType(rulesByKey map[string][]evaluator.EventRuleForEvaluation) ruleSplit {
	seenIDs := make(map[string]bool)
	var definitionRules, geofenceRules []evaluator.EventRuleForEvaluation

	for _, rules := range rulesByKey {
		for _, rule := range rules {
			uniqueKey := r.getRuleUniqueKey(rule)
			if uniqueKey == "" || seenIDs[uniqueKey] {
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

	return ruleSplit{
		definitionRules: definitionRules,
		geofenceRules:   geofenceRules,
	}
}

// getRuleUniqueKey returns a unique key for deduplication
func (r *RuleRegistry) getRuleUniqueKey(rule evaluator.EventRuleForEvaluation) string {
	if rule.GeofenceID != nil && *rule.GeofenceID != "" {
		return *rule.GeofenceID
	}
	if rule.AutomationID != "" {
		return rule.AutomationID
	}
	return rule.EventRuleID
}

// evaluateDefinitionRules evaluates definition-based rules and returns matched events
func (r *RuleRegistry) evaluateDefinitionRules(rules []evaluator.EventRuleForEvaluation, deviceID string, entities []models.TelemetryEntity, matchedRuleKeys map[string]bool) []models.MatchedEvent {
	var matchedEvents []models.MatchedEvent

	for _, rule := range rules {
		matched := r.evaluator.EvaluateRuleDBWithEntities(rule, deviceID, entities, map[string]interface{}{}, map[string]interface{}{})
		ruleKey := r.getRuleKey(rule)

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

	return matchedEvents
}

// getRuleKey safely extracts the rule key from a rule
func (r *RuleRegistry) getRuleKey(rule evaluator.EventRuleForEvaluation) string {
	if rule.RuleKey != nil {
		return *rule.RuleKey
	}
	return ""
}

// evaluateDefaultRules evaluates default system rules for entities that haven't been matched by custom rules
func (r *RuleRegistry) evaluateDefaultRules(brand, model string, entities []models.TelemetryEntity, matchedRuleKeys map[string]bool, deviceID string) []models.MatchedEvent {
	var matchedEvents []models.MatchedEvent

	r.defaultRulesMu.RLock()
	key := fmt.Sprintf("%s/%s", strings.ToLower(brand), strings.ToLower(model))
	defaultRulesByKey, exists := r.groupedDefaultRules[key]
	r.defaultRulesMu.RUnlock()

	if !exists {
		return matchedEvents
	}

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

	return matchedEvents
}

// geofenceResult holds the result of a geofence check
type geofenceResult struct {
	rule     evaluator.EventRuleForEvaluation
	isInside bool
	typeZone string
}

// evaluatedResult holds an evaluated geofence result with definition matching info
type evaluatedResult struct {
	res               geofenceResult
	definitionMatched bool
	hasDefinition     bool
}

// evaluateGeofenceRules evaluates geofence-based rules and returns matched events
func (r *RuleRegistry) evaluateGeofenceRules(ctx context.Context, rules []evaluator.EventRuleForEvaluation, deviceID string, entities []models.TelemetryEntity, matchedRuleKeys map[string]bool) []models.MatchedEvent {
	lat, lon, locationStateID, hasLocation := extractLocation(entities)
	if !hasLocation {
		return nil
	}

	r.logger.Info("Evaluating geofence rules with location",
		zap.String("device_id", deviceID),
		zap.Float64("lat", lat),
		zap.Float64("lon", lon),
		zap.Int("geofence_rule_count", len(rules)))

	results := r.checkGeofences(ctx, rules, deviceID, lat, lon)
	evaluated := r.evaluateGeofenceDefinitions(ctx, results, deviceID, lat, lon, entities)

	isInsideAnySafeZone := r.checkSafeZoneStatus(evaluated)
	if isInsideAnySafeZone {
		r.logger.Info("Device is inside a safe zone (with conditions met), will skip all safe zone exit events",
			zap.String("device_id", deviceID))
	}

	return r.buildGeofenceEvents(evaluated, deviceID, lat, lon, locationStateID, isInsideAnySafeZone, matchedRuleKeys)
}

// checkGeofences checks all geofences and returns the results
func (r *RuleRegistry) checkGeofences(ctx context.Context, rules []evaluator.EventRuleForEvaluation, deviceID string, lat, lon float64) []geofenceResult {
	var results []geofenceResult

	for _, rule := range rules {
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

		results = append(results, geofenceResult{rule: rule, isInside: isInside, typeZone: typeZone})
	}

	return results
}

// evaluateGeofenceDefinitions evaluates definition conditions for geofence rules
func (r *RuleRegistry) evaluateGeofenceDefinitions(ctx context.Context, results []geofenceResult, deviceID string, lat, lon float64, entities []models.TelemetryEntity) []evaluatedResult {
	var evaluated []evaluatedResult

	for _, res := range results {
		rule := res.rule
		geofenceIDDebug := ""
		if rule.GeofenceID != nil {
			geofenceIDDebug = *rule.GeofenceID
		}

		definitionMatched := false
		hasDefinition := len(rule.Definition) > 0

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

		evaluated = append(evaluated, evaluatedResult{
			res:               res,
			definitionMatched: definitionMatched,
			hasDefinition:     hasDefinition,
		})
	}

	return evaluated
}

// checkSafeZoneStatus checks if device is inside any safe zone with conditions met
func (r *RuleRegistry) checkSafeZoneStatus(evaluated []evaluatedResult) bool {
	for _, ev := range evaluated {
		if ev.res.typeZone == "safe" && ev.res.isInside {
			if ev.hasDefinition {
				if ev.definitionMatched {
					return true
				}
			} else {
				return true
			}
		}
	}
	return false
}

// buildGeofenceEvents builds matched events from evaluated geofence results
func (r *RuleRegistry) buildGeofenceEvents(evaluated []evaluatedResult, deviceID string, lat, lon float64, locationStateID uuid.UUID, isInsideAnySafeZone bool, matchedRuleKeys map[string]bool) []models.MatchedEvent {
	var dangerEvents, safeEvents []models.MatchedEvent

	for _, ev := range evaluated {
		res := ev.res
		rule := res.rule
		geofenceIDDebug := ""
		if rule.GeofenceID != nil {
			geofenceIDDebug = *rule.GeofenceID
		}

		shouldTrigger := r.shouldTriggerGeofence(res.typeZone, res.isInside, ev.hasDefinition, ev.definitionMatched, isInsideAnySafeZone)

		r.logger.Info("Geofence shouldTrigger evaluated",
			zap.String("device_id", deviceID),
			zap.String("geofence_id", geofenceIDDebug),
			zap.String("type_zone", res.typeZone),
			zap.Bool("is_inside", res.isInside),
			zap.Bool("has_definition", ev.hasDefinition),
			zap.Bool("definition_matched", ev.definitionMatched),
			zap.Bool("should_trigger", shouldTrigger))

		if !shouldTrigger {
			continue
		}

		event := r.buildGeofenceEvent(rule, deviceID, lat, lon, locationStateID, res.typeZone, res.isInside)

		// Classify event by zone type
		switch res.typeZone {
		case "danger":
			dangerEvents = append(dangerEvents, event)
		case "safe":
			safeEvents = append(safeEvents, event)
		default:
			dangerEvents = append(dangerEvents, event)
		}
	}

	// Priority: danger events take precedence over safe events
	var geofenceMatchedEvents []models.MatchedEvent
	if len(dangerEvents) > 0 {
		geofenceMatchedEvents = dangerEvents
	} else if len(safeEvents) > 0 {
		geofenceMatchedEvents = safeEvents
	}

	// Mark rule keys as matched
	for _, evt := range geofenceMatchedEvents {
		if evt.RuleKey != "" {
			matchedRuleKeys[evt.RuleKey] = true
		}
	}

	return geofenceMatchedEvents
}

// shouldTriggerGeofence determines if a geofence event should trigger
func (r *RuleRegistry) shouldTriggerGeofence(typeZone string, isInside, hasDefinition, definitionMatched, isInsideAnySafeZone bool) bool {
	switch typeZone {
	case "safe":
		if isInsideAnySafeZone {
			return false
		}
		if hasDefinition {
			return !isInside && definitionMatched
		}
		return !isInside
	default:
		if hasDefinition {
			return definitionMatched
		}
		return isInside
	}
}

// buildGeofenceEvent builds a matched event from geofence data
func (r *RuleRegistry) buildGeofenceEvent(rule evaluator.EventRuleForEvaluation, deviceID string, lat, lon float64, locationStateID uuid.UUID, typeZone string, isInside bool) models.MatchedEvent {
	_, geofenceTitle, eventDesc := r.evaluateGeofenceTrigger(isInside, typeZone, *rule.GeofenceID)

	if rule.Description != nil && *rule.Description != "" {
		eventDesc = *rule.Description
	}

	ruleKey := r.getRuleKey(rule)

	var eventRuleID *string
	if rule.EventRuleID != "" {
		erid := rule.EventRuleID
		eventRuleID = &erid
	}

	var geofenceName *string
	if rule.GeofenceName != "" {
		geofenceName = &rule.GeofenceName
	}

	return models.MatchedEvent{
		DeviceID:     deviceID,
		EntityType:   "location",
		RuleKey:      ruleKey,
		EventType:    "device_event",
		EventLevel:   "automation",
		Title:        geofenceTitle,
		Description:  eventDesc,
		Value:        lat,
		Threshold:    lon,
		Operator:     "geofence:" + typeZone,
		Timestamp:    time.Now().UnixMilli(),
		EventRuleID:  eventRuleID,
		AutomationID: nil,
		GeofenceID:   rule.GeofenceID,
		GeofenceName: geofenceName,
		StateID:      locationStateID,
		Location:     &models.Location{Latitude: lat, Longitude: lon},
	}
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
func extractLocation(entities []models.TelemetryEntity) (lat, lon float64, stateID uuid.UUID, found bool) {
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
	return 0, 0, uuid.UUID{}, false
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
