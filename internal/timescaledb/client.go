package timescaledb

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"strings"
	"sync"
	"time"

	dbpkg "github.com/Space-DF/telemetry-service/pkgs/db"
	dbmodels "github.com/Space-DF/telemetry-service/pkgs/db/models"
	_ "github.com/lib/pq"
	"github.com/stephenafamo/bob"
	"github.com/stephenafamo/bob/dialect/psql"
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
// locationWithOrg wraps a location with its organization context

type Client struct {
	db            bob.DB
	logger        *zap.Logger
	batchSize     int
	flushInterval time.Duration
	connStr       string

	batchCh chan *locationWithOrg
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
		connStr:       connStr,

		batchCh: make(chan *locationWithOrg, batchSize*BatchChannelBufferSize),
	}, nil
}

// AddLocation adds a location to the batch

// GetLocationHistory retrieves location history for a device
func (c *Client) GetLocationHistory(ctx context.Context, deviceID, spaceSlug string, start, end time.Time, limit int) ([]*dbmodels.DeviceLocation, error) {
	// Extract org from context and set search_path in transaction if provided
	org := orgFromContext(ctx)

	// Log the incoming query context for debugging multi-tenant behavior
	if c.logger != nil {
		c.logger.Info("GetLocationHistory called",
			zap.String("org_from_ctx", org),
			zap.String("space_slug_param", spaceSlug),
			zap.String("device_id", deviceID),
			zap.Time("start", start),
			zap.Time("end", end),
			zap.Int("limit", limit),
		)
	}

	// Also print to stdout for quick debug visibility in container logs
	log.Printf("GetLocationHistory called - org='%s' space_slug='%s' device_id='%s' start='%s' end='%s' limit=%d",
		org, spaceSlug, deviceID, start.String(), end.String(), limit)

	if org != "" {
		var locations []*dbmodels.DeviceLocation
		err := c.withOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
			var qerr error
			locations, qerr = dbmodels.DeviceLocations.Query(
				sm.Where(dbmodels.DeviceLocations.Columns.DeviceID.EQ(psql.Arg(deviceID))),
				sm.Where(dbmodels.DeviceLocations.Columns.SpaceSlug.EQ(psql.Arg(spaceSlug))),
				sm.Where(dbmodels.DeviceLocations.Columns.Time.GTE(psql.Arg(start))),
				sm.Where(dbmodels.DeviceLocations.Columns.Time.LTE(psql.Arg(end))),
				sm.OrderBy(dbmodels.DeviceLocations.Columns.Time).Asc(),
				sm.Limit(limit),
			).All(txCtx, tx)
			return qerr
		})
		if err != nil {
			return nil, fmt.Errorf("failed to query location history: %w", err)
		}

		if c.logger != nil {
			c.logger.Info("GetLocationHistory result", zap.Int("rows", len(locations)), zap.String("org", org))
		}
		log.Printf("GetLocationHistory result - org='%s' rows=%d", org, len(locations))

		return locations, nil
	}

	// Query using Bob ORM without transaction (uses default search_path)
	locations, err := dbmodels.DeviceLocations.Query(
		sm.Where(dbmodels.DeviceLocations.Columns.DeviceID.EQ(psql.Arg(deviceID))),
		sm.Where(dbmodels.DeviceLocations.Columns.SpaceSlug.EQ(psql.Arg(spaceSlug))),
		sm.Where(dbmodels.DeviceLocations.Columns.Time.GTE(psql.Arg(start))),
		sm.Where(dbmodels.DeviceLocations.Columns.Time.LTE(psql.Arg(end))),
		sm.OrderBy(dbmodels.DeviceLocations.Columns.Time).Asc(),
		sm.Limit(limit),
	).All(ctx, c.db)

	if err != nil {
		return nil, fmt.Errorf("failed to query location history: %w", err)
	}

	if c.logger != nil {
		c.logger.Info("GetLocationHistory result (no-org)", zap.Int("rows", len(locations)), zap.String("org", ""))
	}
	log.Printf("GetLocationHistory result (no-org) - rows=%d", len(locations))

	return locations, nil
}

// GetLastLocation retrieves the most recent location for a device
func (c *Client) GetLastLocation(ctx context.Context, deviceID, spaceSlug string) (*dbmodels.DeviceLocation, error) {
	// Extract org from context and set search_path in transaction if provided
	org := orgFromContext(ctx)

	if c.logger != nil {
		c.logger.Info("GetLastLocation called",
			zap.String("org_from_ctx", org),
			zap.String("space_slug_param", spaceSlug),
			zap.String("device_id", deviceID),
		)
	}
	log.Printf("GetLastLocation called - org='%s' space_slug='%s' device_id='%s'", org, spaceSlug, deviceID)

	if org != "" {
		var location *dbmodels.DeviceLocation
		err := c.withOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
			var qerr error
			location, qerr = dbmodels.DeviceLocations.Query(
				sm.Where(dbmodels.DeviceLocations.Columns.DeviceID.EQ(psql.Arg(deviceID))),
				sm.Where(dbmodels.DeviceLocations.Columns.SpaceSlug.EQ(psql.Arg(spaceSlug))),
				sm.OrderBy(dbmodels.DeviceLocations.Columns.Time).Desc(),
				sm.Limit(1),
			).One(txCtx, tx)
			return qerr
		})
		if err != nil {
			return nil, fmt.Errorf("failed to query last location: %w", err)
		}

		if c.logger != nil {
			c.logger.Info("GetLastLocation result", zap.Bool("found", location != nil), zap.String("org", org))
		}
		log.Printf("GetLastLocation result - org='%s' found=%t", org, location != nil)

		return location, nil
	}

	// Query using Bob ORM without transaction (uses default search_path)
	location, err := dbmodels.DeviceLocations.Query(
		sm.Where(dbmodels.DeviceLocations.Columns.DeviceID.EQ(psql.Arg(deviceID))),
		sm.Where(dbmodels.DeviceLocations.Columns.SpaceSlug.EQ(psql.Arg(spaceSlug))),
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

// CreateSchema creates a PostgreSQL schema for the given organization if it doesn't exist.
// The orgSlug is escaped to avoid breaking SQL; identifiers cannot be parameterized,
// so we replace any double-quotes with two double-quotes and wrap the identifier in quotes.
func (c *Client) CreateSchema(ctx context.Context, orgSlug string) error {
	if orgSlug == "" {
		return fmt.Errorf("empty organization slug")
	}

	// Escape any double quotes in the identifier
	escaped := strings.ReplaceAll(orgSlug, `"`, `""`)
	query := fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS "%s"`, escaped)

	// Use ExecContext on the underlying DB to run the DDL
	if _, err := c.db.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("failed to create schema '%s': %w", orgSlug, err)
	}

	c.logger.Info("Ensured database schema for organization", zap.String("org", orgSlug))
	return nil
}

// CreateSchemaAndTables ensures the schema exists and creates required tables
// for telemetry within that schema: device_locations and schema_migrations.
func (c *Client) CreateSchemaAndTables(ctx context.Context, orgSlug string) error {
	// Create the schema first
	if err := c.CreateSchema(ctx, orgSlug); err != nil {
		return err
	}

	// Build a connection URL that sets the search_path to the new schema so migrations run inside it
	if c.connStr == "" {
		return fmt.Errorf("no connection string available to run migrations")
	}

	parsed, err := url.Parse(c.connStr)
	if err != nil {
		return fmt.Errorf("failed to parse connection string for migrations: %w", err)
	}

	q := parsed.Query()
	// Set options to set the search_path to the new schema and public when connecting
	// This ensures functions/extensions (like TimescaleDB functions installed in public)
	// remain visible to the migration SQL that may call them.
	// e.g. options=-c search_path=org_slug,public
	q.Set("options", fmt.Sprintf("-c search_path=%s,public", orgSlug))
	parsed.RawQuery = q.Encode()

	migrationPath := "pkgs/db/migrations"

	// Run migrations for this schema using the project's db migration helper
	if err := dbpkg.Migrate(parsed, migrationPath); err != nil {
		return fmt.Errorf("failed to run migrations for schema '%s': %w", orgSlug, err)
	}

	c.logger.Info("Ran migrations for organization schema", zap.String("org", orgSlug))
	return nil
}

// DropSchema drops a PostgreSQL schema for the given organization.
// It uses CASCADE to remove all objects in the schema. This should be
// used with caution — consumers should ensure no active connections or
// processing is ongoing for the organization before calling this.
func (c *Client) DropSchema(ctx context.Context, orgSlug string) error {
	if orgSlug == "" {
		return fmt.Errorf("empty organization slug")
	}

	// Escape any double quotes in the identifier
	escaped := strings.ReplaceAll(orgSlug, `"`, `""`)
	query := fmt.Sprintf(`DROP SCHEMA IF EXISTS "%s" CASCADE`, escaped)

	if _, err := c.db.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("failed to drop schema '%s': %w", orgSlug, err)
	}

	c.logger.Info("Dropped database schema for organization", zap.String("org", orgSlug))
	return nil
}
