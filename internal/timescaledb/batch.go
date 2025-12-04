package timescaledb

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	dbmodels "github.com/Space-DF/telemetry-service/pkgs/db/models"
	"github.com/aarondl/opt/omit"
	"github.com/stephenafamo/bob"
	"github.com/stephenafamo/bob/dialect/psql/im"
	"go.uber.org/zap"
)

// locationWithOrg wraps a location with its organization context
type locationWithOrg struct {
	location *dbmodels.DeviceLocationSetter
	org      string
}

// AddLocation adds a location to the batch
func (c *Client) AddLocation(ctx context.Context, location *dbmodels.DeviceLocationSetter) error {
	ctx, cancel := context.WithTimeout(ctx, DropTimeout)
	defer cancel()

	org := orgFromContext(ctx)

	// If the incoming location setter doesn't include the SpaceSlug, populate it
	// from the context so the stored row has the correct space_slug value.
	if org != "" {
		if location != nil && !location.SpaceSlug.IsValue() {
			location.SpaceSlug = omit.From(org)
		}
	}

	for {
		select {
		case <-ctx.Done():
			if err := ctx.Err(); err != nil && errors.Is(err, context.DeadlineExceeded) {
				return ErrLocationDroppedTimeout
			}
			return nil
		case c.batchCh <- &locationWithOrg{location: location, org: org}:
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
			batch := make([]*locationWithOrg, 0, c.batchSize)

			for {
				select {
				case locWithOrg := <-c.batchCh:
					batch = append(batch, locWithOrg)
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
	batch := make([]*locationWithOrg, batchSize)
	for i := 0; i < batchSize; i++ {
		batch[i] = <-c.batchCh
	}
	return c.insertBatch(context.Background(), batch)
}

// insertBatch inserts a batch of locations into Psql using Bob ORM
func (c *Client) insertBatch(ctx context.Context, batch []*locationWithOrg) error {
	if len(batch) == 0 {
		return nil
	}

	// Group locations by org to batch them into transactions per org
	locsByOrg := make(map[string][]*dbmodels.DeviceLocationSetter)
	for _, item := range batch {
		locsByOrg[item.org] = append(locsByOrg[item.org], item.location)
	}

	// Insert each org's locations in a separate transaction with proper search_path
	for org, locations := range locsByOrg {
		// use helper to manage tx lifecycle and org search_path
		err := c.withOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
			_, err := dbmodels.DeviceLocations.Insert(
				bob.ToMods(locations...),
				im.OnConflict("time", "device_id").DoNothing(),
			).Exec(txCtx, tx)
			if err != nil {
				return fmt.Errorf("failed to insert batch for org '%s': %w", org, err)
			}

			c.logger.Info("Successfully inserted location batch",
				zap.String("org", org),
				zap.Int("batch_size", len(locations)),
			)
			log.Printf("Inserted batch - org='%s' batch_size=%d", org, len(locations))
			return nil
		})
		if err != nil {
			return err
		}
	}

	return nil
}
