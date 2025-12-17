package read

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/Space-DF/telemetry-service/internal/timescaledb/core"
	"github.com/lib/pq"
	"github.com/stephenafamo/bob"
	"go.uber.org/zap"
)

// Service groups read-side queries.
type Service struct {
	base *core.Base
}

func NewService(base *core.Base) *Service {
	return &Service{base: base}
}

// Location represents a device location record.
type Location struct {
	Time       time.Time
	DeviceID   string
	SpaceSlug  string
	Latitude   float64
	Longitude  float64
	Attributes map[string]interface{}
}

type rowScanner interface {
	Scan(dest ...any) error
}

type rowsIterator interface {
	rowScanner
	Next() bool
	Err() error
	Close() error
}

// GetLocationHistory retrieves location history for a device.
func (s *Service) GetLocationHistory(ctx context.Context, deviceID, spaceSlug string, start, end time.Time, limit int) ([]*Location, error) {
	org := core.OrgFromContext(ctx)

	if logger := s.base.Logger(); logger != nil {
		logger.Info("GetLocationHistory called",
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

	var (
		locations []*Location
		err       error
	)

	if org != "" {
		err = s.base.WithOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
			rows, qerr := tx.QueryContext(txCtx, query, deviceID, spaceSlug, start, end, limit)
			if qerr != nil {
				return qerr
			}

			locs, scanErr := collectLocations(rows)
			if scanErr != nil {
				return scanErr
			}
			locations = locs
			return nil
		})
	} else {
		rows, qerr := s.base.DB().QueryContext(ctx, query, deviceID, spaceSlug, start, end, limit)
		if qerr != nil {
			return nil, qerr
		}

		locations, err = collectLocations(rows)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to query location history: %w", err)
	}

	if logger := s.base.Logger(); logger != nil {
		logger.Info("GetLocationHistory result", zap.Int("rows", len(locations)), zap.String("org", org))
	}
	log.Printf("GetLocationHistory result - org='%s' rows=%d", org, len(locations))

	return locations, nil
}

// GetLastLocation retrieves the most recent location for a device.
func (s *Service) GetLastLocation(ctx context.Context, deviceID, spaceSlug string) (*Location, error) {
	org := core.OrgFromContext(ctx)

	if logger := s.base.Logger(); logger != nil {
		logger.Info("GetLastLocation called",
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

	var (
		location *Location
		err      error
	)

	if org != "" {
		err = s.base.WithOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
			row := tx.QueryRowContext(txCtx, query, deviceID, spaceSlug)
			loc, scanErr := decodeLocation(row)
			if errors.Is(scanErr, sql.ErrNoRows) {
				return nil
			}
			if scanErr != nil {
				return scanErr
			}
			location = loc
			return nil
		})
	} else {
		row := s.base.DB().QueryRowContext(ctx, query, deviceID, spaceSlug)
		loc, scanErr := decodeLocation(row)
		if errors.Is(scanErr, sql.ErrNoRows) {
			return nil, nil
		}
		if scanErr != nil {
			return nil, scanErr
		}
		location = loc
	}

	if err != nil {
		return nil, fmt.Errorf("failed to query last location: %w", err)
	}

	if logger := s.base.Logger(); logger != nil {
		logger.Info("GetLastLocation result", zap.Bool("found", location != nil), zap.String("org", org))
	}
	log.Printf("GetLastLocation result - org='%s' found=%t", org, location != nil)

	return location, nil
}

func collectLocations(rows rowsIterator) ([]*Location, error) {
	defer func() { _ = rows.Close() }()

	var locations []*Location
	for rows.Next() {
		loc, err := decodeLocation(rows)
		if err != nil {
			return nil, err
		}
		locations = append(locations, loc)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}
	return locations, nil
}

func decodeLocation(scanner rowScanner) (*Location, error) {
	var (
		t       pq.NullTime
		did     sql.NullString
		sslug   sql.NullString
		rawAttr []byte
	)

	if err := scanner.Scan(&t, &did, &sslug, &rawAttr); err != nil {
		return nil, err
	}

	attrs := parseLocationAttributes(rawAttr)
	lat, lon := extractCoordinates(attrs)

	return &Location{
		Time:       t.Time,
		DeviceID:   did.String,
		SpaceSlug:  sslug.String,
		Latitude:   lat,
		Longitude:  lon,
		Attributes: attrs,
	}, nil
}

func parseLocationAttributes(rawAttr []byte) map[string]interface{} {
	if len(rawAttr) == 0 {
		return nil
	}

	var attrs map[string]interface{}
	if err := json.Unmarshal(rawAttr, &attrs); err != nil {
		return nil
	}
	return attrs
}

func extractCoordinates(attrs map[string]interface{}) (float64, float64) {
	if attrs == nil {
		return 0, 0
	}

	var lat, lon float64
	if l, ok := attrs["latitude"].(float64); ok {
		lat = l
	}
	if l, ok := attrs["longitude"].(float64); ok {
		lon = l
	}
	return lat, lon
}
