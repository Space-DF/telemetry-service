package timescaledb

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/url"
	"strings"
	"sync"
	"time"

	alertregistry "github.com/Space-DF/telemetry-service/internal/alerts/registry"
	dbpkg "github.com/Space-DF/telemetry-service/pkgs/db"
	"github.com/lib/pq"
	"github.com/stephenafamo/bob"
	"go.uber.org/zap"
)

type Location struct {
	Time       time.Time
	DeviceID   string
	SpaceSlug  string
	Latitude   float64
	Longitude  float64
	Attributes map[string]interface{}
}

const BatchChannelBufferSize = 10
const DropTimeout = 1 * time.Second

var ErrLocationDroppedTimeout = fmt.Errorf("location dropped due to timeout")

type ErrDroppedBatch struct {
	Size int
}

func (e *ErrDroppedBatch) Error() string {
	return fmt.Sprintf("batch dropped due to timeout, size: %d", e.Size)
}

var (
	ErrDateRequired      = errors.New("date is required")
	ErrInvalidDateFormat = errors.New("invalid date format, expected YYYY-MM-DD")
)

// Client represents a Psql client
type Client struct {
	db            bob.DB
	logger        *zap.Logger
	batchSize     int
	flushInterval time.Duration
	connStr       string

	wg sync.WaitGroup
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
	}, nil
}

// AddLocation adds a location to the batch

// GetLocationHistory retrieves location history for a device
func (c *Client) GetLocationHistory(ctx context.Context, deviceID, spaceSlug string, start, end time.Time, limit int) ([]*Location, error) {
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

	// SQL to reconstruct locations from the entities schema
	// Filter by category='location' to only get location entities
	query := `SELECT s.reported_at, e.device_id::text, e.space_slug, a.shared_attrs
		FROM entity_states s
		JOIN entities e ON s.entity_id = e.id
		LEFT JOIN entity_state_attributes a ON s.attributes_id = a.id
		WHERE e.device_id::text = $1 AND e.space_slug = $2 
			AND e.category = 'location'
			AND s.reported_at >= $3 AND s.reported_at <= $4
			AND a.shared_attrs IS NOT NULL
			AND a.shared_attrs ? 'latitude' AND a.shared_attrs ? 'longitude'
		ORDER BY s.reported_at ASC
		LIMIT $5`

	locations := make([]*Location, 0)
	var err error
	if org != "" {
		err = c.withOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
			rows, qerr := tx.QueryContext(txCtx, query, deviceID, spaceSlug, start, end, limit)
			if qerr != nil {
				return qerr
			}
			defer func() { _ = rows.Close() }()

			for rows.Next() {
				var t pq.NullTime
				var did sql.NullString
				var sslug sql.NullString
				var rawAttrs []byte
				if err := rows.Scan(&t, &did, &sslug, &rawAttrs); err != nil {
					return err
				}
				attrs := map[string]interface{}(nil)
				if len(rawAttrs) > 0 {
					var m map[string]interface{}
					if jerr := json.Unmarshal(rawAttrs, &m); jerr == nil {
						attrs = m
					}
				}
				var lat, lon float64
				if attrs != nil {
					if l, ok := attrs["latitude"].(float64); ok {
						lat = l
					}
					if l, ok := attrs["longitude"].(float64); ok {
						lon = l
					}
				}
				loc := &Location{
					Time:       t.Time,
					DeviceID:   did.String,
					SpaceSlug:  sslug.String,
					Latitude:   lat,
					Longitude:  lon,
					Attributes: attrs,
				}
				locations = append(locations, loc)
			}
			return rows.Err()
		})
	} else {
		rows, err := c.db.QueryContext(ctx, query, deviceID, spaceSlug, start, end, limit)
		if err != nil {
			return nil, err
		}
		defer func() { _ = rows.Close() }()

		for rows.Next() {
			var t pq.NullTime
			var did sql.NullString
			var sslug sql.NullString
			var rawAttrs []byte
			if err := rows.Scan(&t, &did, &sslug, &rawAttrs); err != nil {
				return nil, err
			}
			attrs := map[string]interface{}(nil)
			if len(rawAttrs) > 0 {
				var m map[string]interface{}
				if jerr := json.Unmarshal(rawAttrs, &m); jerr == nil {
					attrs = m
				}
			}
			var lat, lon float64
			if attrs != nil {
				if l, ok := attrs["latitude"].(float64); ok {
					lat = l
				}
				if l, ok := attrs["longitude"].(float64); ok {
					lon = l
				}
			}
			loc := &Location{
				Time:       t.Time,
				DeviceID:   did.String,
				SpaceSlug:  sslug.String,
				Latitude:   lat,
				Longitude:  lon,
				Attributes: attrs,
			}
			locations = append(locations, loc)
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}

	if err != nil {
		return nil, fmt.Errorf("failed to query location history: %w", err)
	}

	if c.logger != nil {
		c.logger.Info("GetLocationHistory result", zap.Int("rows", len(locations)), zap.String("org", org))
	}
	log.Printf("GetLocationHistory result - org='%s' rows=%d", org, len(locations))

	return locations, nil
}

// GetLastLocation retrieves the most recent location for a device
func (c *Client) GetLastLocation(ctx context.Context, deviceID, spaceSlug string) (*Location, error) {
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

	// Filter by category='location' to only get location entities
	query := `SELECT s.reported_at, e.device_id::text, e.space_slug, a.shared_attrs
		FROM entity_states s
		JOIN entities e ON s.entity_id = e.id
		LEFT JOIN entity_state_attributes a ON s.attributes_id = a.id
		WHERE e.device_id::text = $1 AND e.space_slug = $2
			AND e.category = 'location'
			AND a.shared_attrs IS NOT NULL
			AND a.shared_attrs ? 'latitude' AND a.shared_attrs ? 'longitude'
		ORDER BY s.reported_at DESC
		LIMIT 1`

	var location *Location
	var err error
	if org != "" {
		err = c.withOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
			row := tx.QueryRowContext(txCtx, query, deviceID, spaceSlug)
			var t pq.NullTime
			var did sql.NullString
			var sslug sql.NullString
			var rawAttrs []byte
			if err := row.Scan(&t, &did, &sslug, &rawAttrs); err != nil {
				if err == sql.ErrNoRows {
					return nil
				}
				return err
			}
			attrs := map[string]interface{}(nil)
			if len(rawAttrs) > 0 {
				var m map[string]interface{}
				if jerr := json.Unmarshal(rawAttrs, &m); jerr == nil {
					attrs = m
				}
			}
			var lat, lon float64
			if attrs != nil {
				if l, ok := attrs["latitude"].(float64); ok {
					lat = l
				}
				if l, ok := attrs["longitude"].(float64); ok {
					lon = l
				}
			}
			location = &Location{
				Time:       t.Time,
				DeviceID:   did.String,
				SpaceSlug:  sslug.String,
				Latitude:   lat,
				Longitude:  lon,
				Attributes: attrs,
			}
			return nil
		})
	} else {
		row := c.db.QueryRowContext(ctx, query, deviceID, spaceSlug)
		var t pq.NullTime
		var did sql.NullString
		var sslug sql.NullString
		var rawAttrs []byte
		if err := row.Scan(&t, &did, &sslug, &rawAttrs); err != nil {
			if err == sql.ErrNoRows {
				return nil, nil
			}
			return nil, err
		}
		attrs := map[string]interface{}(nil)
		if len(rawAttrs) > 0 {
			var m map[string]interface{}
			if jerr := json.Unmarshal(rawAttrs, &m); jerr == nil {
				attrs = m
			}
		}
		var lat, lon float64
		if attrs != nil {
			if l, ok := attrs["latitude"].(float64); ok {
				lat = l
			}
			if l, ok := attrs["longitude"].(float64); ok {
				lon = l
			}
		}
		location = &Location{
			Time:       t.Time,
			DeviceID:   did.String,
			SpaceSlug:  sslug.String,
			Latitude:   lat,
			Longitude:  lon,
			Attributes: attrs,
		}
	}

	if err != nil {
		return nil, fmt.Errorf("failed to query last location: %w", err)
	}

	if c.logger != nil {
		c.logger.Info("GetLastLocation result", zap.Bool("found", location != nil), zap.String("org", org))
	}
	log.Printf("GetLastLocation result - org='%s' found=%t", org, location != nil)

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

// GetEntities returns entities for a given space with optional filters and pagination.
func (c *Client) GetEntities(ctx context.Context, spaceSlug, category, deviceID string, displayTypes []string, search string, page, pageSize int) ([]map[string]interface{}, int, error) {
	org := orgFromContext(ctx)

	if page < 1 {
		page = 1
	}
	if pageSize <= 0 {
		pageSize = 100
	}
	offset := (page - 1) * pageSize

	// Build WHERE clauses
	args := []interface{}{spaceSlug}
	where := "e.space_slug = $1"
	idx := 2
	if category != "" {
		where += fmt.Sprintf(" AND e.category = $%d", idx)
		args = append(args, category)
		idx++
	}
	if deviceID != "" {
		where += fmt.Sprintf(" AND e.device_id = $%d", idx)
		args = append(args, deviceID)
		idx++
	}
	if len(displayTypes) > 0 {
		where += fmt.Sprintf(" AND e.display_type @> $%d::text[]", idx)
		args = append(args, pq.Array(displayTypes))
		idx++
	}
	if search != "" {
		searchPattern := "%" + search + "%"
		where += fmt.Sprintf(" AND (e.name ILIKE $%[1]d OR e.unique_key ILIKE $%[1]d OR e.category ILIKE $%[1]d OR e.device_id::text ILIKE $%[1]d OR et.name ILIKE $%[1]d OR et.unique_key ILIKE $%[1]d)", idx)
		args = append(args, searchPattern)
		idx++
	}

	// Count query
	countQuery := fmt.Sprintf("SELECT COUNT(1) FROM entities e LEFT JOIN entity_types et ON e.entity_type_id = et.id WHERE %s", where)
	var total int
	if org != "" {
		if err := c.withOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
			row := tx.QueryRowContext(txCtx, countQuery, args...)
			return row.Scan(&total)
		}); err != nil {
			return nil, 0, fmt.Errorf("failed to count entities: %w", err)
		}
	} else {
		row := c.db.QueryRowContext(ctx, countQuery, args...)
		if err := row.Scan(&total); err != nil {
			return nil, 0, fmt.Errorf("failed to count entities: %w", err)
		}
	}

	// Select query
	selectQuery := fmt.Sprintf(`SELECT e.id, e.device_id, e.name, e.unique_key, et.id AS entity_type_id, et.name AS entity_type_name, et.unique_key AS entity_type_unique_key, et.image_url AS entity_type_image_url, e.category, e.unit_of_measurement, e.display_type, e.image_url, e.is_enabled, e.created_at, e.updated_at, s.time_start, s.time_end
		FROM entities e
		LEFT JOIN entity_types et ON e.entity_type_id = et.id
		LEFT JOIN (
			SELECT entity_id, MIN(reported_at) AS time_start, MAX(reported_at) AS time_end FROM entity_states GROUP BY entity_id
		) s ON s.entity_id = e.id
		WHERE %s
		ORDER BY e.created_at DESC
		LIMIT $%d OFFSET $%d`, where, idx, idx+1)

	args = append(args, pageSize, offset)

	// Run query
	var results []map[string]interface{}
	if org != "" {
		if err := c.withOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
			rows, err := tx.QueryContext(txCtx, selectQuery, args...)
			if err != nil {
				return err
			}
			defer func() {
				_ = rows.Close()
			}()

			for rows.Next() {
				var id, deviceIDCol, name, uniqueKey sql.NullString
				var etID, etName, etUnique, etImage sql.NullString
				var categoryCol, unit, imageURL sql.NullString
				var displayType pq.StringArray
				var isEnabled bool
				var createdAt, updatedAt pq.NullTime
				var timeStart, timeEnd pq.NullTime

				if err := rows.Scan(&id, &deviceIDCol, &name, &uniqueKey, &etID, &etName, &etUnique, &etImage, &categoryCol, &unit, &displayType, &imageURL, &isEnabled, &createdAt, &updatedAt, &timeStart, &timeEnd); err != nil {
					return err
				}

				rowMap := map[string]interface{}{
					"id":          id.String,
					"device_id":   deviceIDCol.String,
					"device_name": name.String,
					"unique_key":  uniqueKey.String,
					"entity_type": map[string]interface{}{
						"id":         etID.String,
						"name":       etName.String,
						"unique_key": etUnique.String,
						"image_url":  etImage.String,
					},
					"name":                name.String,
					"category":            categoryCol.String,
					"unit_of_measurement": unit.String,
					"display_type":        []string(displayType),
					"image_url":           imageURL.String,
					"is_enabled":          isEnabled,
					"created_at":          createdAt.Time,
					"updated_at":          updatedAt.Time,
					"time_start":          timeStart.Time,
					"time_end":            timeEnd.Time,
				}
				results = append(results, rowMap)
			}
			return nil
		}); err != nil {
			return nil, 0, err
		}
	} else {
		// Query directly using bob.DB's QueryContext
		rows, err := c.db.QueryContext(ctx, selectQuery, args...)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to query entities: %w", err)
		}
		defer func() {
			_ = rows.Close()
		}()

		for rows.Next() {
			var id, deviceIDCol, name, uniqueKey sql.NullString
			var etID, etName, etUnique, etImage sql.NullString
			var categoryCol, unit, imageURL sql.NullString
			var displayType pq.StringArray
			var isEnabled bool
			var createdAt, updatedAt pq.NullTime
			var timeStart, timeEnd pq.NullTime

			if err := rows.Scan(&id, &deviceIDCol, &name, &uniqueKey, &etID, &etName, &etUnique, &etImage, &categoryCol, &unit, &displayType, &imageURL, &isEnabled, &createdAt, &updatedAt, &timeStart, &timeEnd); err != nil {
				return nil, 0, err
			}

			rowMap := map[string]interface{}{
				"id":          id.String,
				"device_id":   deviceIDCol.String,
				"device_name": name.String,
				"unique_key":  uniqueKey.String,
				"entity_type": map[string]interface{}{
					"id":         etID.String,
					"name":       etName.String,
					"unique_key": etUnique.String,
					"image_url":  etImage.String,
				},
				"name":                name.String,
				"category":            categoryCol.String,
				"unit_of_measurement": unit.String,
				"display_type":        []string(displayType),
				"image_url":           imageURL.String,
				"is_enabled":          isEnabled,
				"created_at":          createdAt.Time,
				"updated_at":          updatedAt.Time,
				"time_start":          timeStart.Time,
				"time_end":            timeEnd.Time,
			}
			results = append(results, rowMap)
		}
	}

	return results, total, nil
}

// GetLatestAttributesForDeviceAt returns the shared attributes JSON for the
// given device at or before the provided timestamp. If there are no
// attributes available it returns (nil, nil).
func (c *Client) GetLatestAttributesForDeviceAt(ctx context.Context, deviceID string, at time.Time) (map[string]interface{}, error) {
	org := orgFromContext(ctx)

	// SQL selects the shared_attrs JSON stored in entity_state_attributes
	// joined through entity_states -> entities filtered by device_id and
	// reported_at <= provided time, ordered by reported_at desc.
	query := `SELECT a.shared_attrs
		FROM entities e
		JOIN entity_states s ON s.entity_id = e.id
		LEFT JOIN entity_state_attributes a ON s.attributes_id = a.id
		WHERE e.device_id::text = $1 AND s.reported_at <= $2 AND a.shared_attrs IS NOT NULL
		ORDER BY s.reported_at DESC
		LIMIT 1`

	var rawAttrs []byte
	if org != "" {
		if err := c.withOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
			rows, err := tx.QueryContext(txCtx, query, deviceID, at)
			if err != nil {
				return err
			}
			defer func() {
				_ = rows.Close()
			}()
			if rows.Next() {
				return rows.Scan(&rawAttrs)
			}
			return nil
		}); err != nil {
			return nil, fmt.Errorf("failed to query attributes: %w", err)
		}
	} else {
		rows, err := c.db.QueryContext(ctx, query, deviceID, at)
		if err != nil {
			return nil, fmt.Errorf("failed to query attributes: %w", err)
		}
		defer func() {
			_ = rows.Close()
		}()
		if rows.Next() {
			if err := rows.Scan(&rawAttrs); err != nil {
				return nil, err
			}
		}
	}

	if len(rawAttrs) == 0 {
		return nil, nil
	}

	var attrs map[string]interface{}
	if err := json.Unmarshal(rawAttrs, &attrs); err != nil {
		return nil, fmt.Errorf("failed to unmarshal attributes JSON: %w", err)
	}

	return attrs, nil
}

// GetAlerts retrieves alerts for a device/category within a single day.
func (c *Client) GetAlerts(ctx context.Context, orgSlug, category, spaceSlug, deviceID, dateStr string, warningThreshold, criticalThreshold float64, page, pageSize int) ([]interface{}, int, error) {
	org := orgSlug
	if org == "" {
		org = orgFromContext(ctx)
	}
	if org == "" || spaceSlug == "" || deviceID == "" {
		return nil, 0, fmt.Errorf("org, space_slug, and device_id are required")
	}

	offset := (page - 1) * pageSize

	processor, ok := alertregistry.Get(category)
	if !ok {
		return nil, 0, fmt.Errorf("unsupported category: %s", category)
	}

	// Apply processor defaults if not provided
	if warningThreshold <= 0 {
		warningThreshold = processor.DefaultWarningThreshold()
	}
	if criticalThreshold <= 0 {
		criticalThreshold = processor.DefaultCriticalThreshold()
	}

	startAt, endAt, err := buildDateRange(dateStr)
	if err != nil {
		return nil, 0, err
	}

	args := []interface{}{category, spaceSlug, deviceID, startAt, endAt, pageSize, offset}
	countArgs := args[:5]

	statePredicate := processor.StatePredicate()
	if strings.TrimSpace(statePredicate) == "" {
		statePredicate = "TRUE"
	}
	whereClause := fmt.Sprintf(`
		e.is_enabled = true
		AND e.category = $1
		AND e.space_slug = $2
		AND e.device_id = $3
		AND %s
		AND s.reported_at >= $4
		AND s.reported_at < $5
	`, statePredicate)

	query := fmt.Sprintf(alertsQueryTemplate, whereClause)

	// Count query
	countQuery := fmt.Sprintf(alertsCountQueryTemplate, whereClause)

	var totalCount int
	var results []interface{}

	if err := c.withOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
		// Get total count
		row := tx.QueryRowContext(txCtx, countQuery, countArgs...)
		if err := row.Scan(&totalCount); err != nil {
			return fmt.Errorf("failed to count alerts: %w", err)
		}

		// Get alerts
		rows, err := tx.QueryContext(txCtx, query, args...)
		if err != nil {
			return fmt.Errorf("failed to query alerts: %w", err)
		}
		defer func() {
			_ = rows.Close()
		}()

		for rows.Next() {
			var entityID, entityName, deviceIDVal, spaceSlugVal, state string
			var reportedAt time.Time
			var attributesID sql.NullString
			var latitude, longitude sql.NullFloat64

			if err := rows.Scan(&entityID, &entityName, &deviceIDVal, &spaceSlugVal, &state, &reportedAt, &attributesID, &latitude, &longitude); err != nil {
				return fmt.Errorf("failed to scan alert row: %w", err)
			}

			value := 0.0
			if parsed, err := processor.ParseValue(state); err == nil {
				value = parsed
			}

			levelComputed := processor.DetermineLevel(value, warningThreshold, criticalThreshold)
			alert := map[string]interface{}{
				"id":                 entityID,
				"type":               processor.DetermineType(value, warningThreshold, criticalThreshold),
				"level":              levelComputed,
				"message":            processor.GenerateMessage(levelComputed, value),
				"entity_id":          entityID,
				"entity_name":        entityName,
				"device_id":          deviceIDVal,
				"space_slug":         spaceSlugVal,
				processor.ValueKey(): value,
				"unit":               processor.Unit(),
				"threshold": map[string]interface{}{
					"warning":  warningThreshold,
					"critical": criticalThreshold,
				},
				"reported_at": reportedAt,
			}

			// Add location if available
			if latitude.Valid && longitude.Valid {
				alert["location"] = map[string]interface{}{
					"latitude":  latitude.Float64,
					"longitude": longitude.Float64,
				}
			}

			// Add attributes if available
			if attributesID.Valid {
				attrs, err := getAttributesByID(txCtx, tx, attributesID.String)
				if err == nil && attrs != nil {
					alert["attributes"] = attrs
				}
			}

			results = append(results, alert)
		}

		return rows.Err()
	}); err != nil {
		return nil, 0, err
	}

	return results, totalCount, nil
}

// buildDateRange converts a required YYYY-MM-DD string into a single-day [start, end) window.
func buildDateRange(dateStr string) (time.Time, time.Time, error) {
	const layout = "2006-01-02" // Go's reference layout meaning YYYY-MM-DD
	trimmed := strings.TrimSpace(dateStr)
	if trimmed == "" {
		return time.Time{}, time.Time{}, fmt.Errorf("%w", ErrDateRequired)
	}

	parsed, err := time.Parse(layout, trimmed)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("%w", ErrInvalidDateFormat)
	}

	start := parsed.UTC()
	end := start.Add(24 * time.Hour)
	return start, end, nil
}

func getAttributesByID(ctx context.Context, tx bob.Tx, attributesID string) (map[string]interface{}, error) {
	query := `SELECT shared_attrs FROM entity_state_attributes WHERE id = $1`
	var rawAttrs []byte

	row := tx.QueryRowContext(ctx, query, attributesID)
	if err := row.Scan(&rawAttrs); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	if len(rawAttrs) == 0 {
		return nil, nil
	}

	var attrs map[string]interface{}
	if err := json.Unmarshal(rawAttrs, &attrs); err != nil {
		return nil, err
	}

	return attrs, nil
}

const alertsQueryTemplate = `
	SELECT 
		e.id as entity_id,
		e.name as entity_name,
		e.device_id,
		e.space_slug,
		s.state,
		s.reported_at,
		s.attributes_id,
		loc.latitude,
		loc.longitude
	FROM entities e
	INNER JOIN entity_states s ON s.entity_id = e.id
	LEFT JOIN LATERAL (
		SELECT e2.id, s2.state
		FROM entities e2
		INNER JOIN entity_states s2 ON s2.entity_id = e2.id
		WHERE e2.device_id = e.device_id
			AND e2.category = 'location'
			AND e2.is_enabled = true
		ORDER BY s2.reported_at DESC
		LIMIT 1
	) loc_entity ON true
	LEFT JOIN LATERAL (
		SELECT 
			CASE 
				WHEN loc_entity.state ~ '^\\s*\\{.*\\}\\s*$' 
					AND loc_entity.state ~ '\"latitude\"' 
					AND loc_entity.state ~ '\"longitude\"'
				THEN (loc_entity.state::jsonb->>'latitude')::float 
			END as latitude,
			CASE 
				WHEN loc_entity.state ~ '^\\s*\\{.*\\}\\s*$' 
					AND loc_entity.state ~ '\"latitude\"' 
					AND loc_entity.state ~ '\"longitude\"'
				THEN (loc_entity.state::jsonb->>'longitude')::float 
			END as longitude
	) loc ON true
	WHERE %s
	ORDER BY s.reported_at DESC
	LIMIT $6 OFFSET $7
`

const alertsCountQueryTemplate = `
	SELECT COUNT(*) 
	FROM entities e
	INNER JOIN entity_states s ON s.entity_id = e.id
	WHERE %s
`
