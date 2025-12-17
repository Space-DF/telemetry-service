package read

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/Space-DF/telemetry-service/internal/timescaledb/core"
	"github.com/stephenafamo/bob"
	"go.uber.org/zap"
)

// GetDeviceProperties retrieves the latest properties (location + categories) for a device.
func (s *Service) GetDeviceProperties(ctx context.Context, deviceID, spaceSlug string) (map[string]interface{}, error) {
	org := core.OrgFromContext(ctx)
	if org == "" {
		return nil, fmt.Errorf("organization not found in context")
	}

	props := make(map[string]interface{})

	location, err := s.GetLastLocation(ctx, deviceID, spaceSlug)
	if err == nil && location != nil {
		props["latest_checkpoint"] = map[string]interface{}{
			"timestamp": location.Time,
			"latitude":  location.Latitude,
			"longitude": location.Longitude,
		}
	}

	err = s.base.WithOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
		rows, err := tx.QueryContext(txCtx, `
			SELECT DISTINCT e.category
			FROM entities e
			WHERE e.device_id::text = $1 AND e.space_slug = $2 AND e.category != 'location'
			ORDER BY e.category
		`, deviceID, spaceSlug)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()

		var entityCategories []string
		for rows.Next() {
			var category string
			if err := rows.Scan(&category); err != nil {
				return err
			}
			entityCategories = append(entityCategories, category)
		}
		if err := rows.Err(); err != nil {
			return err
		}

		for _, category := range entityCategories {
			row := tx.QueryRowContext(txCtx, `
				SELECT COALESCE(es.state::float8, 0)
				FROM entity_states es
				JOIN entities e ON es.entity_id = e.id
				WHERE e.device_id::text = $1 AND e.space_slug = $2 AND e.category = $3
				ORDER BY es.reported_at DESC
				LIMIT 1
			`, deviceID, spaceSlug, category)

			var value float64
			if err := row.Scan(&value); err != nil {
				if !errors.Is(err, sql.ErrNoRows) {
					if logger := s.base.Logger(); logger != nil {
						logger.Warn("Failed to query entity value",
							zap.Error(err),
							zap.String("category", category),
							zap.String("device_id", deviceID),
						)
					}
				}
				continue
			}
			props[category] = value
		}
		return nil
	})

	if err != nil {
		if logger := s.base.Logger(); logger != nil {
			logger.Error("Failed to query device properties",
				zap.Error(err),
				zap.String("device_id", deviceID),
			)
		}
		return props, nil
	}

	return props, nil
}
