package timescaledb

import (
	"fmt"
	"sync"
	"time"

	"github.com/stephenafamo/bob"
	"go.uber.org/zap"
)

// Client represents a TimescaleDB client with basic lifecycle helpers.
type Client struct {
	DB            bob.DB      // Exported for subpackages
	Logger        *zap.Logger // Exported for subpackages
	batchSize     int
	flushInterval time.Duration
	connStr       string

	wg sync.WaitGroup

	// OnGeofenceChange is called after a geofence is created, updated, or deleted.
	// Set this to invalidate caches or trigger side-effects.
	OnGeofenceChange func()

	// RuleRegistry provides access to the device rules cache
	// Set this in the server initialization to enable cache invalidation on automation changes
	RuleRegistry interface{}
}

// NewClient creates a new TimescaleDB client and verifies connectivity.
func NewClient(connStr string, batchSize int, flushInterval time.Duration, logger *zap.Logger) (*Client, error) {
	db, err := bob.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &Client{
		DB:            db,
		Logger:        logger,
		batchSize:     batchSize,
		flushInterval: flushInterval,
		connStr:       connStr,
	}, nil
}

// HealthCheck checks if TimescaleDB is reachable.
func (c *Client) HealthCheck() error {
	return c.DB.Ping()
}

// Wait blocks until all background workers have finished.
func (c *Client) Wait() {
	c.wg.Wait()
}

// Close closes the database connection.
func (c *Client) Close() error {
	return c.DB.Close()
}
