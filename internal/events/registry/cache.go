package registry

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Space-DF/telemetry-service/internal/models"
	"github.com/Space-DF/telemetry-service/internal/timescaledb"
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
	Rules      []models.EventRule                      // Flat array (for compatibility)
	RulesByKey map[string][]models.EventRule           // Grouped by rule_key for O(1) lookup
	CachedAt   time.Time
	ExpiresAt  time.Time
}

// DeviceRulesCache manages caching of device automation rules
type DeviceRulesCache struct {
	cache map[string]*DeviceRulesCacheEntry // key: deviceID
	mu    sync.RWMutex
	ttl   time.Duration
	db    *timescaledb.Client
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

// Get retrieves device automation rules from cache or database
func (c *DeviceRulesCache) Get(ctx context.Context, deviceID string) []models.EventRule {
	now := time.Now()

	// Try cache first (read lock)
	c.mu.RLock()
	entry, found := c.cache[deviceID]
	c.mu.RUnlock()

	if found && now.Before(entry.ExpiresAt) {
		// Cache hit - valid entry
		c.hits.Add(1)
		c.logger.Debug("Device rules cache hit",
			zap.String("device_id", deviceID),
			zap.Int("rule_count", len(entry.Rules)))
		return entry.Rules
	}

	// Cache miss or expired
	c.misses.Add(1)

	if found {
		c.logger.Debug("Device rules cache expired",
			zap.String("device_id", deviceID),
			zap.Time("expired_at", entry.ExpiresAt))
	}

	// Fetch from database
	rules, err := c.db.GetActiveRulesForDevice(ctx, deviceID)
	if err != nil {
		c.logger.Error("Failed to fetch automation rules for device",
			zap.String("device_id", deviceID),
			zap.Error(err))
		return nil
	}

	// Group rules by rule_key for O(1) lookup
	rulesByKey := c.groupRulesByKey(rules)

	// Store in cache (write lock)
	entry = &DeviceRulesCacheEntry{
		Rules:      rules,
		RulesByKey: rulesByKey,
		CachedAt:   now,
		ExpiresAt:  now.Add(c.ttl),
	}

	c.mu.Lock()
	c.cache[deviceID] = entry
	c.mu.Unlock()

	c.logger.Debug("Cached device automation rules",
		zap.String("device_id", deviceID),
		zap.Int("rule_count", len(rules)),
		zap.Duration("ttl", c.ttl))

	return rules
}

// GetGrouped retrieves grouped device automation rules from cache or database
func (c *DeviceRulesCache) GetGrouped(ctx context.Context, deviceID string) map[string][]models.EventRule {
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
	c.Get(ctx, deviceID)

	// Try cache again
	c.mu.RLock()
	entry, found = c.cache[deviceID]
	c.mu.RUnlock()

	if found && now.Before(entry.ExpiresAt) {
		return entry.RulesByKey
	}

	return nil
}

// groupRulesByKey groups rules by their rule_key for O(1) lookup
func (c *DeviceRulesCache) groupRulesByKey(rules []models.EventRule) map[string][]models.EventRule {
	result := make(map[string][]models.EventRule)

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
		"hits":       hits,
		"misses":     misses,
		"total":      total,
		"hit_rate":   hitRate,
		"cache_size": cacheSize,
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
