package timescaledb

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	dbmodels "github.com/Space-DF/telemetry-service/pkgs/db/models"
	_ "github.com/lib/pq"
	"github.com/stephenafamo/bob"
	"github.com/stephenafamo/bob/dialect/psql"
	"github.com/stephenafamo/bob/dialect/psql/im"
	"github.com/stephenafamo/bob/dialect/psql/sm"
	"go.uber.org/zap"
)

const BatchChannelBufferSize = 10
const DropTimeout = 1 * time.Second

var ErrLocationDroppedTimeout = fmt.Errorf("location dropped due to timeout")

type ErrDroppedBatch struct {
	Size int
}

func (e *ErrDroppedBatch) Error() string {
	return fmt.Sprintf("batch dropped due to timeout, size: %d", e.Size)
}

// Client represents a Psql client
type Client struct {
	db            bob.DB
	logger        *zap.Logger
	batchSize     int
	flushInterval time.Duration

	batchCh chan *dbmodels.DeviceLocationSetter
	wg      sync.WaitGroup
}

// NewClient creates a new Psql client
func NewClient(connStr string, batchSize int, flushInterval time.Duration, logger *zap.Logger) (*Client, error) {
	db, err := bob.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	// Test connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &Client{
		db:            db,
		logger:        logger,
		batchSize:     batchSize,
		flushInterval: flushInterval,

		batchCh: make(chan *dbmodels.DeviceLocationSetter, batchSize*BatchChannelBufferSize),
	}, nil
}

// AddLocation adds a location to the batch
func (c *Client) AddLocation(ctx context.Context, location *dbmodels.DeviceLocationSetter) error {
	ctx, cancel := context.WithTimeout(ctx, DropTimeout)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			if err := ctx.Err(); err != nil && errors.Is(err, context.DeadlineExceeded) {
				return ErrLocationDroppedTimeout
			}
			return nil
		case c.batchCh <- location:
			return nil
		}
	}
}

// StartBatchWriter starts the background batch writer
func (c *Client) StartBatchWriter(ctx context.Context) {
	c.wg.Add(1)
	defer c.wg.Done()

	t := time.NewTicker(c.flushInterval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			c.logger.Info("Stopping batch writer")
			// First flush any pending batch using the regular flush logic
			if err := c.flushBatch(); err != nil {
				c.logger.Error("Failed to flush batch on shutdown", zap.Error(err))
			}

			// Create a timeout context for draining remaining locations
			drainCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			c.logger.Info("Draining batch channel",
				zap.Int("buffer_size", len(c.batchCh)))

			// Drain remaining locations with proper batching
			drained := 0
			batch := make([]*dbmodels.DeviceLocationSetter, 0, c.batchSize)

			for {
				select {
				case location := <-c.batchCh:
					batch = append(batch, location)
					drained++

					// When batch is full, insert it
					if len(batch) >= c.batchSize {
						if err := c.insertBatch(drainCtx, batch); err != nil {
							c.logger.Error("Failed to insert batch during shutdown",
								zap.Error(err),
								zap.Int("batch_size", len(batch)),
								zap.Int("drained_count", drained))
						} else {
							c.logger.Info("Inserted batch during shutdown",
								zap.Int("batch_size", len(batch)),
								zap.Int("drained_count", drained))
						}
						// Reset batch for next set of locations
						batch = batch[:0]
					}
				default:
					// No more locations in channel, insert final partial batch if any
					if len(batch) > 0 {
						if err := c.insertBatch(drainCtx, batch); err != nil {
							c.logger.Error("Failed to insert final batch during shutdown",
								zap.Error(err),
								zap.Int("batch_size", len(batch)))
						} else {
							c.logger.Info("Inserted final batch during shutdown",
								zap.Int("batch_size", len(batch)))
						}
					}
					c.logger.Info("Batch channel drained",
						zap.Int("total_drained", drained))
					return
				}
			}
		case <-t.C:
			if len(c.batchCh) == 0 {
				continue
			}
			err := c.flushBatch()
			if err != nil {
				c.logger.Error("Failed to flush batch", zap.Error(err))
			}
		}

	}
}

// flushBatch sends the current batch for insertion
func (c *Client) flushBatch() error {
	batchSize := c.batchSize
	n := len(c.batchCh)
	if n < c.batchSize {
		batchSize = n
	}
	batch := make([]*dbmodels.DeviceLocationSetter, batchSize)
	for i := 0; i < batchSize; i++ {
		batch[i] = <-c.batchCh
	}
	return c.insertBatch(context.Background(), batch)
}

// insertBatch inserts a batch of locations into Psql using Bob ORM
func (c *Client) insertBatch(ctx context.Context, batch []*dbmodels.DeviceLocationSetter) error {
	if len(batch) == 0 {
		return nil
	}

	// Insert using Bob's batch insert with ToMods
	_, err := dbmodels.DeviceLocations.Insert(
		bob.ToMods(batch...),
		im.OnConflict("time", "device_id").DoNothing(),
	).Exec(ctx, c.db)
	if err != nil {
		return fmt.Errorf("failed to insert batch: %w", err)
	}

	c.logger.Info("Successfully inserted location batch",
		zap.Int("batch_size", len(batch)),
	)

	return nil
}

// GetLocationHistory retrieves location history for a device
func (c *Client) GetLocationHistory(ctx context.Context, deviceID, organizationSlug string, start, end time.Time, limit int) ([]*dbmodels.DeviceLocation, error) {
	// Query using Bob ORM
	locations, err := dbmodels.DeviceLocations.Query(
		sm.Where(dbmodels.DeviceLocations.Columns.DeviceID.EQ(psql.Arg(deviceID))),
		sm.Where(dbmodels.DeviceLocations.Columns.OrganizationSlug.EQ(psql.Arg(organizationSlug))),
		sm.Where(dbmodels.DeviceLocations.Columns.Time.GTE(psql.Arg(start))),
		sm.Where(dbmodels.DeviceLocations.Columns.Time.LTE(psql.Arg(end))),
		sm.OrderBy(dbmodels.DeviceLocations.Columns.Time).Asc(),
		sm.Limit(limit),
	).All(ctx, c.db)

	if err != nil {
		return nil, fmt.Errorf("failed to query location history: %w", err)
	}

	return locations, nil
}

// GetLastLocation retrieves the most recent location for a device
func (c *Client) GetLastLocation(ctx context.Context, deviceID, organizationSlug string) (*dbmodels.DeviceLocation, error) {
	location, err := dbmodels.DeviceLocations.Query(
		sm.Where(dbmodels.DeviceLocations.Columns.DeviceID.EQ(psql.Arg(deviceID))),
		sm.Where(dbmodels.DeviceLocations.Columns.OrganizationSlug.EQ(psql.Arg(organizationSlug))),
		sm.OrderBy(dbmodels.DeviceLocations.Columns.Time).Desc(),
		sm.Limit(1),
	).One(ctx, c.db)

	if err != nil {
		return nil, fmt.Errorf("failed to query last location: %w", err)
	}

	return location, nil
}

// HealthCheck checks if Psql is reachable
func (c *Client) HealthCheck() error {
	return c.db.Ping()
}

// Wait waits for the batch writer to finish
func (c *Client) Wait() {
	c.wg.Wait()
}

// Close closes the database connection
func (c *Client) Close() error {
	return c.db.Close()
}
