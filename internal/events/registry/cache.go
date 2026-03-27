package registry

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Space-DF/telemetry-service/internal/events/evaluator"
	"github.com/Space-DF/telemetry-service/internal/models"
	"github.com/Space-DF/telemetry-service/internal/timescaledb"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

const (
	// DefaultCacheTTL is the default time-to-live for cached device rules
	DefaultCacheTTL = 5 * time.Minute
	// DefaultCacheCleanupInterval is how often to clean expired cache entries
	DefaultCacheCleanupInterval = 1 * time.Minute
)

// DeviceRulesCacheEntry represents a cached entry for device automation rules
type DeviceRulesCacheEntry struct {
	Rules      []evaluator.EventRuleForEvaluation            // Flat array (for compatibility)
	RulesByKey map[string][]evaluator.EventRuleForEvaluation // Grouped by rule_key for O(1) lookup
	CachedAt   time.Time
	ExpiresAt  time.Time
}

// DeviceRulesCache manages caching of device automation rules
type DeviceRulesCache struct {
	cache  map[string]*DeviceRulesCacheEntry // key: deviceID
	mu     sync.RWMutex
	ttl    time.Duration
	db     *timescaledb.Client
	logger *zap.Logger

	// Metrics (atomic counters for thread safety)
	hits   atomic.Int64
	misses atomic.Int64

	// Cleanup management
	ticker  *time.Ticker
	stopped chan struct{}
}

// NewDeviceRulesCache creates a new device rules cache
func NewDeviceRulesCache(db *timescaledb.Client, logger *zap.Logger) *DeviceRulesCache {
	return &DeviceRulesCache{
		cache:   make(map[string]*DeviceRulesCacheEntry),
		ttl:     DefaultCacheTTL,
		ticker:  time.NewTicker(DefaultCacheCleanupInterval),
		stopped: make(chan struct{}),
		db:      db,
		logger:  logger,
	}
}

// Start begins the background cleanup goroutine
func (c *DeviceRulesCache) Start() {
	go func() {
		for {
			select {
			case <-c.ticker.C:
				c.cleanupExpired()
			case <-c.stopped:
				c.ticker.Stop()
				return
			}
		}
	}()
}

// Stop stops the cache cleanup goroutine
func (c *DeviceRulesCache) Stop() {
	close(c.stopped)
}

// Get fetches device automation and geofence rules from database and populates cache
func (c *DeviceRulesCache) Get(ctx context.Context, deviceID string) []evaluator.EventRuleForEvaluation {
	now := time.Now()

	// Fetch geofences for the device's space(s)
	// First get space IDs directly from the entities table for this device
	var geofences []models.GeofenceWithSpace
	spacesSeen := make(map[uuid.UUID]bool)

	deviceSpaceIDs, err := c.db.GetSpaceIDsByDeviceID(ctx, deviceID)
	if err != nil {
		c.logger.Warn("Failed to fetch space IDs for device",
			zap.String("device_id", deviceID),
			zap.Error(err))
	}
	for _, sid := range deviceSpaceIDs {
		spacesSeen[sid] = true
	}

	// Fetch automations for this device for each spaceID
	var automations []models.AutomationWithActions
	for _, spaceID := range deviceSpaceIDs {
		autos, _, err := c.db.GetAutomations(ctx, spaceID, &deviceID, []bool{}, "", 1000, 0)
		if err != nil {
			c.logger.Error("Failed to fetch automations for device in space",
				zap.String("device_id", deviceID),
				zap.String("space_id", spaceID.String()),
				zap.Error(err))
			continue
		}
		automations = append(automations, autos...)
	}

	// Also collect space IDs from automations (in case they differ)
	for _, auto := range automations {
		if auto.SpaceID != nil {
			spacesSeen[*auto.SpaceID] = true
		}
	}

	// Fetch geofences for all discovered spaces
	for sid := range spacesSeen {
		gfs, err := c.db.GetGeofencesBySpace(ctx, sid)
		if err != nil {
			c.logger.Warn("Failed to fetch geofences for space",
				zap.String("space_id", sid.String()),
				zap.Error(err))
			continue
		}
		geofences = append(geofences, gfs...)
	}

	// Collect all event_rule_ids from automations and geofences
	var eventRuleIDs []string
	for _, auto := range automations {
		if auto.EventRuleID != nil {
			eventRuleIDs = append(eventRuleIDs, *auto.EventRuleID)
		}
	}
	for _, gf := range geofences {
		if gf.EventRuleID != nil {
			eventRuleIDs = append(eventRuleIDs, gf.EventRuleID.String())
		}
	}

	// Fetch only the needed event rules from database by IDs
	eventRules, err := c.db.GetEventRulesByIDs(ctx, eventRuleIDs)
	if err != nil {
		c.logger.Error("Failed to fetch event rules",
			zap.Error(err))
		return nil
	}

	// Build a map of event rules by ID for quick lookup
	eventRuleMap := make(map[string]models.EventRule)
	for _, rule := range eventRules {
		eventRuleMap[rule.EventRuleID] = rule
	}

	// Convert automations to EventRuleForEvaluation
	rules := c.convertAutomationsToRules(automations, eventRuleMap)

	// Convert geofences to EventRuleForEvaluation
	geofenceRules := c.convertGeofencesToRules(geofences, eventRuleMap)
	rules = append(rules, geofenceRules...)

	// Group rules by rule_key for O(1) lookup
	rulesByKey := c.groupRulesByKey(rules)

	// Store in cache (write lock)
	entry := &DeviceRulesCacheEntry{
		Rules:      rules,
		RulesByKey: rulesByKey,
		CachedAt:   now,
		ExpiresAt:  now.Add(c.ttl),
	}

	c.mu.Lock()
	c.cache[deviceID] = entry
	c.mu.Unlock()

	c.logger.Debug("Cached device automation and geofence rules",
		zap.String("device_id", deviceID),
		zap.Int("automation_count", len(automations)),
		zap.Int("geofence_count", len(geofences)),
		zap.Int("total_rules", len(rules)),
		zap.Duration("ttl", c.ttl))

	return rules
}

// GetGrouped retrieves grouped device automation rules from cache or database
func (c *DeviceRulesCache) GetGrouped(ctx context.Context, deviceID string) map[string][]evaluator.EventRuleForEvaluation {
	now := time.Now()

	// Try cache first (read lock)
	c.mu.RLock()
	entry, found := c.cache[deviceID]
	c.mu.RUnlock()

	if found && now.Before(entry.ExpiresAt) {
		// Cache hit
		c.hits.Add(1)
		return entry.RulesByKey
	}

	// Cache miss - call Get() which will populate the cache
	c.misses.Add(1)
	rules := c.Get(ctx, deviceID)

	// Return from cache if Get() successfully populated it
	c.mu.RLock()
	entry, found = c.cache[deviceID]
	c.mu.RUnlock()

	if found {
		return entry.RulesByKey // Use pre-computed grouping
	}

	return c.groupRulesByKey(rules)
}

// convertAutomationsToRules converts automations to EventRuleForEvaluation
func (c *DeviceRulesCache) convertAutomationsToRules(automations []models.AutomationWithActions, eventRuleMap map[string]models.EventRule) []evaluator.EventRuleForEvaluation {
	var rules []evaluator.EventRuleForEvaluation

	for _, auto := range automations {
		if auto.EventRuleID == nil {
			continue
		}

		// Look up the event rule from the map
		eventRule, found := eventRuleMap[*auto.EventRuleID]
		if !found {
			c.logger.Warn("Event rule not found for automation",
				zap.String("automation_id", auto.ID),
				zap.String("event_rule_id", *auto.EventRuleID))
			continue
		}

		ruleKey := eventRule.RuleKey
		rule := evaluator.EventRuleForEvaluation{
			EventRuleID:    eventRule.EventRuleID, // actual event_rule UUID
			AutomationID:   auto.ID,               // automation UUID
			AutomationName: auto.Name,             // automation name used as event title
			RuleKey:        &ruleKey,
			Definition:     eventRule.Definition,
			IsActive:       eventRule.IsActive,
			RepeatAble:     eventRule.RepeatAble,
			Description:    eventRule.Description,
			GeofenceID:     nil,
			IsAutomation:   true,
		}

		rules = append(rules, rule)
	}

	return rules
}

// convertGeofencesToRules converts geofences to EventRuleForEvaluation
func (c *DeviceRulesCache) convertGeofencesToRules(geofences []models.GeofenceWithSpace, eventRuleMap map[string]models.EventRule) []evaluator.EventRuleForEvaluation {
	var rules []evaluator.EventRuleForEvaluation

	for _, gf := range geofences {
		if gf.EventRuleID == nil {
			continue
		}

		// Look up the event rule from the map
		eventRule, found := eventRuleMap[gf.EventRuleID.String()]
		if !found {
			c.logger.Warn("Event rule not found for geofence",
				zap.String("geofence_id", gf.GeofenceID.String()),
				zap.String("event_rule_id", gf.EventRuleID.String()))
			continue
		}

		ruleKey := eventRule.RuleKey
		geofenceID := gf.GeofenceID.String()
		rule := evaluator.EventRuleForEvaluation{
			EventRuleID:  eventRule.EventRuleID,
			AutomationID: "",
			RuleKey:      &ruleKey,
			Definition:   eventRule.Definition,
			IsActive:     eventRule.IsActive,
			RepeatAble:   eventRule.RepeatAble,
			Description:  eventRule.Description,
			GeofenceID:   &geofenceID,
			IsAutomation: false,
		}

		rules = append(rules, rule)
	}

	return rules
}

// groupRulesByKey groups rules by their rule_key for O(1) lookup
func (c *DeviceRulesCache) groupRulesByKey(rules []evaluator.EventRuleForEvaluation) map[string][]evaluator.EventRuleForEvaluation {
	result := make(map[string][]evaluator.EventRuleForEvaluation)

	for _, rule := range rules {
		if rule.RuleKey != nil && *rule.RuleKey != "" {
			result[*rule.RuleKey] = append(result[*rule.RuleKey], rule)
		}
	}

	return result
}

// Invalidate removes cached rules for a specific device
// Call this when automation rules are created, updated, or deleted
func (c *DeviceRulesCache) Invalidate(deviceID string) {
	c.mu.Lock()
	delete(c.cache, deviceID)
	c.mu.Unlock()

	c.logger.Info("Invalidated device automation rules cache",
		zap.String("device_id", deviceID))
}

// InvalidateAll clears the entire cache
func (c *DeviceRulesCache) InvalidateAll() {
	c.mu.Lock()
	c.cache = make(map[string]*DeviceRulesCacheEntry)
	c.mu.Unlock()

	c.logger.Info("Invalidated all device automation rules cache")
}

// GetMetrics returns cache performance metrics
func (c *DeviceRulesCache) GetMetrics() map[string]interface{} {
	hits := c.hits.Load()
	misses := c.misses.Load()
	total := hits + misses

	hitRate := float64(0)
	if total > 0 {
		hitRate = float64(hits) / float64(total) * 100
	}

	c.mu.RLock()
	cacheSize := len(c.cache)
	c.mu.RUnlock()

	return map[string]interface{}{
		"hits":        hits,
		"misses":      misses,
		"total":       total,
		"hit_rate":    hitRate,
		"cache_size":  cacheSize,
		"ttl_seconds": c.ttl.Seconds(),
	}
}

// cleanupExpired removes expired entries from the cache
func (c *DeviceRulesCache) cleanupExpired() {
	now := time.Now()
	expiredCount := 0

	c.mu.Lock()
	defer c.mu.Unlock()

	for deviceID, entry := range c.cache {
		if now.After(entry.ExpiresAt) {
			delete(c.cache, deviceID)
			expiredCount++
		}
	}

	if expiredCount > 0 {
		c.logger.Debug("Cleaned expired device rules cache entries",
			zap.Int("expired_count", expiredCount),
			zap.Int("remaining_cache_size", len(c.cache)))
	}
}
