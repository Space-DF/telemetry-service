package timescaledb

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/Space-DF/telemetry-service/internal/api/common"
	"github.com/lib/pq"
	"github.com/stephenafamo/bob"
)

// GetEntities returns entities for a given space with optional filters and pagination.
func (c *Client) GetEntities(ctx context.Context, spaceSlug, category, deviceID string, displayTypes []string, search string, limit, offset int) ([]map[string]interface{}, int, error) {
	org := orgFromContext(ctx)

	if limit <= 0 {
		limit = common.DefaultLimit
	}
	if offset < 0 {
		offset = 0
	}

	// Build WHERE clauses
	args := []interface{}{spaceSlug}
	where := "s.space_slug = $1"
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
		where += fmt.Sprintf(" AND e.display_type::text[] && $%d::text[]", idx)
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
	countQuery := fmt.Sprintf("SELECT COUNT(1) FROM entities e LEFT JOIN entity_types et ON e.entity_type_id = et.id LEFT JOIN spaces s ON e.space_id = s.space_id WHERE %s", where)
	var total int
	if org != "" {
		if err := c.WithOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
			row := tx.QueryRowContext(txCtx, countQuery, args...)
			return row.Scan(&total)
		}); err != nil {
			return nil, 0, fmt.Errorf("failed to count entities: %w", err)
		}
	} else {
		row := c.DB.QueryRowContext(ctx, countQuery, args...)
		if err := row.Scan(&total); err != nil {
			return nil, 0, fmt.Errorf("failed to count entities: %w", err)
		}
	}

	// Select query
	selectQuery := fmt.Sprintf(`SELECT e.id, e.device_id, e.name, e.unique_key, et.id AS entity_type_id, et.name AS entity_type_name, et.unique_key AS entity_type_unique_key, et.image_url AS entity_type_image_url, e.category, e.unit_of_measurement, e.display_type, e.image_url, e.is_enabled, e.created_at, e.updated_at, s2.time_start, s2.time_end
		FROM entities e
		LEFT JOIN entity_types et ON e.entity_type_id = et.id
		LEFT JOIN spaces s ON e.space_id = s.space_id
		LEFT JOIN (
			SELECT entity_id, MIN(reported_at) AS time_start, MAX(reported_at) AS time_end FROM entity_states GROUP BY entity_id
		) s2 ON s2.entity_id = e.id
		WHERE %s
		ORDER BY e.created_at DESC
		LIMIT $%d OFFSET $%d`, where, idx, idx+1)

	args = append(args, limit, offset)

	// Run query
	var results []map[string]interface{}
	if org != "" {
		if err := c.WithOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
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
				var createdAt, updatedAt sql.NullTime
				var timeStart, timeEnd sql.NullTime

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
		rows, err := c.DB.QueryContext(ctx, selectQuery, args...)
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
			var createdAt, updatedAt sql.NullTime
			var timeStart, timeEnd sql.NullTime

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
