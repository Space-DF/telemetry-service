package timescaledb

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Space-DF/telemetry-service/internal/models"
	"github.com/stephenafamo/bob"
	"go.uber.org/zap"
)

type EventPublisher interface {
	PublishEventToDevice(ctx context.Context, event *models.Event, orgSlug string) error
}

// Client represents a TimescaleDB client with basic lifecycle helpers.
type Client struct {
	DB            bob.DB      // Exported for subpackages
	Logger        *zap.Logger // Exported for subpackages
	batchSize     int
	flushInterval time.Duration
	connStr       string
	// PublishEventFunc is an optional hook used to publish real-time events to AMQP.
	publisher EventPublisher

	wg sync.WaitGroup

	// OnGeofenceChange is called after a geofence is created, updated, or deleted.
	// Set this to invalidate caches or trigger side-effects.
	OnGeofenceChange func()
}

// Option is a functional option for configuring Client.
type Option func(*Client)

// WithPublisher sets the event publisher for real-time events.
func WithPublisher(publisher EventPublisher) Option {
	return func(c *Client) {
		c.publisher = publisher
	}
}

// NewClient creates a new TimescaleDB client and verifies connectivity.
func NewClient(connStr string, batchSize int, flushInterval time.Duration, logger *zap.Logger, opts ...Option) (*Client, error) {
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

	client := &Client{
		DB:            db,
		Logger:        logger,
		batchSize:     batchSize,
		flushInterval: flushInterval,
		connStr:       connStr,
	}

	for _, opt := range opts {
		opt(client)
	}

	return client, nil
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

func (c *Client) PublishEventToDevice(ctx context.Context, event *models.Event, orgSlug string) error {
	if c.publisher == nil {
		return nil
	}

	return c.publisher.PublishEventToDevice(ctx, event, orgSlug)
}

// SetPublisher sets the event publisher for real-time events.
// Required for rule actions to publish events. Must be called after consumer initialization.
func (c *Client) SetPublisher(publisher EventPublisher) {
	c.publisher = publisher
}
