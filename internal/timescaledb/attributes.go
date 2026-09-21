package timescaledb

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/stephenafamo/bob"
	"go.uber.org/zap"
)

type Location struct {
	Time       time.Time
	DeviceID   string
	SpaceSlug  string
	Latitude   float64
	Longitude  float64
	Bearing    *float64
	Attributes map[string]interface{}
}

// GetLocationHistory retrieves location history for a device.
func (c *Client) GetLocationHistory(ctx context.Context, deviceID string, start, end time.Time, limit int) ([]*Location, error) {
	org := orgFromContext(ctx)

	if c.Logger != nil {
		c.Logger.Info("GetLocationHistory called",
			zap.String("org_from_ctx", org),
			zap.String("device_id", deviceID),
			zap.Time("start", start),
			zap.Time("end", end),
			zap.Int("limit", limit),
		)
	}

	log.Printf("GetLocationHistory called - org='%s' device_id='%s' start='%s' end='%s' limit=%d",
		org, deviceID, start.String(), end.String(), limit)

	query := `SELECT s.reported_at, e.device_id::text, sp.space_slug, a.shared_attrs
		FROM entity_states s
		JOIN entities e ON s.entity_id = e.id
		LEFT JOIN spaces sp ON e.space_id = sp.space_id
		LEFT JOIN entity_state_attributes a ON s.attributes_id = a.id
		WHERE e.device_id::text = $1
			AND e.category = 'location'
			AND s.reported_at >= $2 AND s.reported_at <= $3
			AND a.shared_attrs IS NOT NULL
			AND a.shared_attrs ? 'latitude' AND a.shared_attrs ? 'longitude'
		ORDER BY s.reported_at ASC
		LIMIT $4`

	locations := make([]*Location, 0)
	if org == "" {
		return nil, fmt.Errorf("organization is required to query location history")
	}

	err := c.WithOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
		rows, qerr := tx.QueryContext(txCtx, query, deviceID, start, end, limit)
		if qerr != nil {
			return qerr
		}
		defer func() { _ = rows.Close() }()

		for rows.Next() {
			var t sql.NullTime
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

	if err != nil {
		return nil, fmt.Errorf("failed to query location history: %w", err)
	}

	if c.Logger != nil {
		c.Logger.Info("GetLocationHistory result", zap.Int("rows", len(locations)), zap.String("org", org))
	}
	log.Printf("GetLocationHistory result - org='%s' rows=%d", org, len(locations))

	return locations, nil
}
