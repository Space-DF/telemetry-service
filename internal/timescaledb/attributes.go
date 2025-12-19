package timescaledb

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/lib/pq"
	"github.com/stephenafamo/bob"
	"go.uber.org/zap"
)

// GetLatestAttributesForDeviceAt returns the shared attributes JSON for the
// given device at or before the provided timestamp. If there are no
// attributes available it returns (nil, nil).
func (c *Client) GetLatestAttributesForDeviceAt(ctx context.Context, deviceID string, at time.Time) (map[string]interface{}, error) {
	org := orgFromContext(ctx)

	query := `SELECT a.shared_attrs
		FROM entities e
		JOIN entity_states s ON s.entity_id = e.id
		LEFT JOIN entity_state_attributes a ON s.attributes_id = a.id
		WHERE e.device_id::text = $1 AND s.reported_at <= $2 AND a.shared_attrs IS NOT NULL
		ORDER BY s.reported_at DESC
		LIMIT 1`

	var rawAttrs []byte
	if org != "" {
		if err := c.WithOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
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
		rows, err := c.DB.QueryContext(ctx, query, deviceID, at)
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

type Location struct {
	Time       time.Time
	DeviceID   string
	SpaceSlug  string
	Latitude   float64
	Longitude  float64
	Attributes map[string]interface{}
}

// GetLocationHistory retrieves location history for a device
func (c *Client) GetLocationHistory(ctx context.Context, deviceID, spaceSlug string, start, end time.Time, limit int) ([]*Location, error) {
	org := orgFromContext(ctx)

	if c.Logger != nil {
		c.Logger.Info("GetLocationHistory called",
			zap.String("org_from_ctx", org),
			zap.String("space_slug_param", spaceSlug),
			zap.String("device_id", deviceID),
			zap.Time("start", start),
			zap.Time("end", end),
			zap.Int("limit", limit),
		)
	}

	log.Printf("GetLocationHistory called - org='%s' space_slug='%s' device_id='%s' start='%s' end='%s' limit=%d",
		org, spaceSlug, deviceID, start.String(), end.String(), limit)

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
		err = c.WithOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
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
		rows, err := c.DB.QueryContext(ctx, query, deviceID, spaceSlug, start, end, limit)
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

	if c.Logger != nil {
		c.Logger.Info("GetLocationHistory result", zap.Int("rows", len(locations)), zap.String("org", org))
	}
	log.Printf("GetLocationHistory result - org='%s' rows=%d", org, len(locations))

	return locations, nil
}

// GetLastLocation retrieves the most recent location for a device
func (c *Client) GetLastLocation(ctx context.Context, deviceID, spaceSlug string) (*Location, error) {
	org := orgFromContext(ctx)

	if c.Logger != nil {
		c.Logger.Info("GetLastLocation called",
			zap.String("org_from_ctx", org),
			zap.String("space_slug_param", spaceSlug),
			zap.String("device_id", deviceID),
		)
	}
	log.Printf("GetLastLocation called - org='%s' space_slug='%s' device_id='%s'", org, spaceSlug, deviceID)

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
		err = c.WithOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
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
		row := c.DB.QueryRowContext(ctx, query, deviceID, spaceSlug)
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

	if c.Logger != nil {
		c.Logger.Info("GetLastLocation result", zap.Bool("found", location != nil), zap.String("org", org))
	}
	log.Printf("GetLastLocation result - org='%s' found=%t", org, location != nil)

	return location, nil
}
