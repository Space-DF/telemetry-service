package timescaledb

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/Space-DF/telemetry-service/internal/api/common"
	"github.com/Space-DF/telemetry-service/internal/client"
	"github.com/google/uuid"
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
	selectQuery := fmt.Sprintf(`SELECT e.id, e.device_id, e.name, e.unique_key, et.id AS entity_type_id, et.name AS entity_type_name, et.unique_key AS entity_type_unique_key, et.image_url AS entity_type_image_url, e.category, e.unit_of_measurement, e.display_type, e.icon, e.is_enabled, e.created_at, e.updated_at, s2.time_start, s2.time_end
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
				var categoryCol, unit, icon sql.NullString
				var displayType pq.StringArray
				var isEnabled bool
				var createdAt, updatedAt sql.NullTime
				var timeStart, timeEnd sql.NullTime

				if err := rows.Scan(&id, &deviceIDCol, &name, &uniqueKey, &etID, &etName, &etUnique, &etImage, &categoryCol, &unit, &displayType, &icon, &isEnabled, &createdAt, &updatedAt, &timeStart, &timeEnd); err != nil {
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
					"icon":                c.ResolveIconURL(icon.String, ""),
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
			var categoryCol, unit, icon sql.NullString
			var displayType pq.StringArray
			var isEnabled bool
			var createdAt, updatedAt sql.NullTime
			var timeStart, timeEnd sql.NullTime

			if err := rows.Scan(&id, &deviceIDCol, &name, &uniqueKey, &etID, &etName, &etUnique, &etImage, &categoryCol, &unit, &displayType, &icon, &isEnabled, &createdAt, &updatedAt, &timeStart, &timeEnd); err != nil {
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
				"icon":                c.ResolveIconURL(icon.String, ""),
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

// GetSpaceIDsByDeviceID returns the distinct space UUIDs associated with a device
func (c *Client) GetSpaceIDsByDeviceID(ctx context.Context, deviceID string) ([]uuid.UUID, error) {
	org := orgFromContext(ctx)
	if org == "" {
		return nil, fmt.Errorf("organization not found in context")
	}

	var spaceIDs []uuid.UUID

	err := c.WithOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
		rows, err := tx.QueryContext(txCtx,
			`SELECT DISTINCT space_id FROM entities WHERE device_id = $1 AND space_id IS NOT NULL`, deviceID)
		if err != nil {
			return fmt.Errorf("failed to query space IDs for device: %w", err)
		}
		defer func() { _ = rows.Close() }()

		for rows.Next() {
			var id uuid.UUID
			if err := rows.Scan(&id); err != nil {
				return err
			}
			spaceIDs = append(spaceIDs, id)
		}
		return rows.Err()
	})

	if err != nil {
		return nil, err
	}
	return spaceIDs, nil
}

func (c *Client) CreateDeviceEntities(ctx context.Context, deviceID, spaceSlug, deviceModel, devEUI string, templates []client.DeviceEntityTemplate) (int64, error) {
	org := orgFromContext(ctx)
	if org == "" {
		return 0, fmt.Errorf("organization not found in context")
	}
	if deviceID == "" || spaceSlug == "" || deviceModel == "" || devEUI == "" {
		return 0, fmt.Errorf("device_id, space_slug, device_model, and dev_eui are required")
	}
	if len(templates) == 0 {
		return 0, nil
	}

	deviceUUID, err := uuid.Parse(deviceID)
	if err != nil {
		return 0, fmt.Errorf("invalid device_id '%s': %w", deviceID, err)
	}

	var createdCount int64
	err = c.WithOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
		var spaceID uuid.UUID
		if err := tx.QueryRowContext(txCtx, `
			SELECT space_id FROM spaces WHERE space_slug = $1 LIMIT 1
		`, spaceSlug).Scan(&spaceID); err != nil {
			return fmt.Errorf("failed to resolve space '%s': %w", spaceSlug, err)
		}

		for _, tpl := range templates {
			displayType := tpl.DisplayType
			if len(displayType) == 0 {
				displayType = []string{"unknown"}
			}

			modelKey := strings.TrimSpace(tpl.ModelKey)
			if modelKey == "" {
				modelKey = deviceModel
			}
			if modelKey == "" {
				return fmt.Errorf("template missing model key for device_model '%s'", deviceModel)
			}

			templateKey := strings.TrimSpace(tpl.Key)
			if templateKey == "" {
				return fmt.Errorf("template missing key for device_model '%s' and model_key '%s'", deviceModel, modelKey)
			}

			entityTypeKey := strings.TrimSpace(tpl.EntityType)
			if entityTypeKey == "" {
				return fmt.Errorf("template missing entity_type for device_model '%s', model_key '%s', and key '%s'", deviceModel, modelKey, templateKey)
			}

			entityUniqueKey := fmt.Sprintf("%s_%s_%s", modelKey, devEUI, templateKey)

			var entityTypeID uuid.UUID
			if err := tx.QueryRowContext(txCtx, `
				INSERT INTO entity_types (id, name, unique_key, image_url, created_at, updated_at)
				VALUES ($1, $2, $3, $4, now(), now())
				ON CONFLICT (unique_key) DO UPDATE SET
					name = EXCLUDED.name,
					image_url = COALESCE(EXCLUDED.image_url, entity_types.image_url),
					updated_at = now()
				RETURNING id
			`,
				uuid.New(),
				tpl.Name,
				entityTypeKey,
				nullString(tpl.Icon),
			).Scan(&entityTypeID); err != nil {
				return fmt.Errorf("upsert entity type '%s': %w", entityTypeKey, err)
			}

			result, err := tx.ExecContext(txCtx, `
				INSERT INTO entities (
					id, space_id, device_id, unique_key, category, entity_type_id,
					name, unit_of_measurement, display_type, icon, is_enabled, created_at, updated_at
				)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, true, now(), now())
				ON CONFLICT (unique_key) DO NOTHING
			`,
				uuid.New(),
				spaceID,
				deviceUUID,
				entityUniqueKey,
				tpl.Category,
				entityTypeID,
				tpl.Name,
				nullString(tpl.UnitOfMeas),
				pq.Array(displayType),
				nullString(tpl.Icon),
			)
			if err != nil {
				return fmt.Errorf("insert entity '%s': %w", entityUniqueKey, err)
			}

			rowsAffected, err := result.RowsAffected()
			if err != nil {
				return fmt.Errorf("entity rows affected for '%s': %w", entityUniqueKey, err)
			}
			createdCount += rowsAffected
		}

		return nil
	})
	if err != nil {
		return 0, err
	}

	return createdCount, nil
}
