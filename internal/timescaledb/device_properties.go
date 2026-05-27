package timescaledb

import (
	"context"
	"fmt"

	"github.com/stephenafamo/bob"
	"go.uber.org/zap"
)

// GetDeviceProperties retrieves all latest properties for a device.
func (c *Client) GetDeviceProperties(ctx context.Context, deviceID string) (map[string]interface{}, error) {
	org := orgFromContext(ctx)
	if org == "" {
		return nil, fmt.Errorf("organization not found in context")
	}

	props := make(map[string]interface{})

	// Get last location
	location, err := c.GetLastLocation(ctx, deviceID)
	if err == nil && location != nil {
		props["latest_checkpoint"] = map[string]interface{}{
			"timestamp": location.Time,
			"latitude":  location.Latitude,
			"longitude": location.Longitude,
			"bearing":   location.Bearing,
		}
	}

	// Get latest values for all entity categories associated with this device
	err = c.WithOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
		query := `
			SELECT DISTINCT e.category
			FROM entities e
			WHERE e.device_id::text = $1 AND e.category != 'location'
			ORDER BY e.category
		`

		rows, err := tx.QueryContext(txCtx, query, deviceID)
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

		// Then get latest value for each category
		for _, category := range entityCategories {
			query := `
				SELECT COALESCE(es.state::float8, 0)
				FROM entity_states es
				JOIN entities e ON es.entity_id = e.id
				WHERE e.device_id::text = $1
				  AND e.category = $2
				ORDER BY es.reported_at DESC
				LIMIT 1
			`

			row := tx.QueryRowContext(txCtx, query, deviceID, category)

			var value float64
			if err := row.Scan(&value); err != nil {
				if err.Error() != "sql: no rows in result set" {
					c.Logger.Warn("Failed to query entity value",
						zap.Error(err),
						zap.String("category", category),
						zap.String("device_id", deviceID),
					)
				}
				continue
			}
			props[category] = value
		}
		return nil
	})

	if err != nil {
		c.Logger.Error("Failed to query device properties",
			zap.Error(err),
			zap.String("device_id", deviceID),
		)
		return props, nil
	}

	return props, nil
}
