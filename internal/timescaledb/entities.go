package timescaledb

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/Space-DF/telemetry-service/internal/api/common"
	"github.com/Space-DF/telemetry-service/internal/client"
	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/stephenafamo/bob"
)

func buildEntityRowMap(c *Client, id, deviceIDCol, name, manufacturer, uniqueKey sql.NullString, etID, etName, etUnique, etImage sql.NullString, categoryCol, unit, icon sql.NullString, displayType pq.StringArray, isEnabled bool, createdAt, updatedAt, timeStart, timeEnd sql.NullTime) map[string]interface{} {
	return map[string]interface{}{
		"id":          id.String,
		"device_id":   deviceIDCol.String,
		"device_name": name.String,
		"manufacturer": manufacturer.String,
		"unique_key":   uniqueKey.String,
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
}

type BulkUpdateEntitiesResult struct {
	Entities     []BulkUpdateEntityResult `json:"entities"`
	UpdatedCount int                      `json:"updated_count"`
}

type BulkUpdateEntityResult struct {
	EntityID  string    `json:"entity_id"`
	IsEnabled bool      `json:"is_enabled"`
	UpdatedAt time.Time `json:"updated_at"`
}

type EntityEnabledUpdate struct {
	EntityID  uuid.UUID
	IsEnabled bool
}

// GetEntities returns entities with optional filters and pagination.
func (c *Client) GetEntities(ctx context.Context, spaceSlug, category, deviceID, devEUI string, displayTypes []string, search string, limit, offset int) ([]map[string]interface{}, int, error) {
	org := orgFromContext(ctx)

	if limit <= 0 {
		limit = common.DefaultLimit
	}
	if offset < 0 {
		offset = 0
	}

	// Build WHERE clauses
	args := []interface{}{}
	conditions := make([]string, 0, 6)
	idx := 1
	if spaceSlug != "" {
		conditions = append(conditions, fmt.Sprintf("s.space_slug = $%d", idx))
		args = append(args, spaceSlug)
		idx++
	}
	if category != "" {
		conditions = append(conditions, fmt.Sprintf("e.category = $%d", idx))
		args = append(args, category)
		idx++
	}
	if deviceID != "" {
		conditions = append(conditions, fmt.Sprintf("e.device_id = $%d", idx))
		args = append(args, deviceID)
		idx++
	}
	if devEUI != "" {
		conditions = append(conditions, fmt.Sprintf("e.unique_key LIKE $%d", idx))
		args = append(args, "%_"+devEUI+"_%")
		idx++
	}
	if len(displayTypes) > 0 {
		conditions = append(
			conditions,
			fmt.Sprintf("e.display_type::text[] && $%d::text[]", idx),
		)
		args = append(args, pq.Array(displayTypes))
		idx++
	}
	if search != "" {
		searchPattern := "%" + search + "%"
		conditions = append(
			conditions,
			fmt.Sprintf("(e.name ILIKE $%[1]d OR e.manufacturer ILIKE $%[1]d OR e.unique_key ILIKE $%[1]d OR e.category ILIKE $%[1]d OR e.device_id::text ILIKE $%[1]d OR et.name ILIKE $%[1]d OR et.unique_key ILIKE $%[1]d)", idx),
		)
		args = append(args, searchPattern)
		idx++
	}
	where := strings.Join(conditions, " AND ")
	if where == "" {
		where = "e.id IS NOT NULL"
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
	selectQuery := fmt.Sprintf(`SELECT e.id, e.device_id, e.name, e.manufacturer, e.unique_key, et.id AS entity_type_id, et.name AS entity_type_name, et.unique_key AS entity_type_unique_key, et.image_url AS entity_type_image_url, e.category, e.unit_of_measurement, e.display_type, e.icon, e.is_enabled, e.created_at, e.updated_at, s2.time_start, s2.time_end
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
			var id, deviceIDCol, name, manufacturer, uniqueKey sql.NullString
			var etID, etName, etUnique, etImage sql.NullString
			var categoryCol, unit, icon sql.NullString
			var displayType pq.StringArray
			var isEnabled bool
			var createdAt, updatedAt sql.NullTime
			var timeStart, timeEnd sql.NullTime

			if err := rows.Scan(&id, &deviceIDCol, &name, &manufacturer, &uniqueKey, &etID, &etName, &etUnique, &etImage, &categoryCol, &unit, &displayType, &icon, &isEnabled, &createdAt, &updatedAt, &timeStart, &timeEnd); err != nil {
				return err
			}

			rowMap := buildEntityRowMap(c, id, deviceIDCol, name, manufacturer, uniqueKey, etID, etName, etUnique, etImage, categoryCol, unit, icon, displayType, isEnabled, createdAt, updatedAt, timeStart, timeEnd)
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
			var id, deviceIDCol, name, manufacturer, uniqueKey sql.NullString
			var etID, etName, etUnique, etImage sql.NullString
			var categoryCol, unit, icon sql.NullString
			var displayType pq.StringArray
			var isEnabled bool
			var createdAt, updatedAt sql.NullTime
			var timeStart, timeEnd sql.NullTime

			if err := rows.Scan(&id, &deviceIDCol, &name, &manufacturer, &uniqueKey, &etID, &etName, &etUnique, &etImage, &categoryCol, &unit, &displayType, &icon, &isEnabled, &createdAt, &updatedAt, &timeStart, &timeEnd); err != nil {
				return nil, 0, err
			}

			rowMap := buildEntityRowMap(c, id, deviceIDCol, name, manufacturer, uniqueKey, etID, etName, etUnique, etImage, categoryCol, unit, icon, displayType, isEnabled, createdAt, updatedAt, timeStart, timeEnd)
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

func (c *Client) UpdateEntities(ctx context.Context, updates []EntityEnabledUpdate, spaceSlug string) (*BulkUpdateEntitiesResult, error) {
	org := orgFromContext(ctx)
	if org == "" {
		return nil, fmt.Errorf("organization not found in context")
	}
	if len(updates) == 0 {
		return nil, fmt.Errorf("updates are required")
	}
	if strings.TrimSpace(spaceSlug) == "" {
		return nil, fmt.Errorf("space_slug is required")
	}

	var result *BulkUpdateEntitiesResult
	err := c.WithOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
		entityIDs := make([]uuid.UUID, 0, len(updates))
		enabledStates := make([]bool, 0, len(updates))
		for _, update := range updates {
			entityIDs = append(entityIDs, update.EntityID)
			enabledStates = append(enabledStates, update.IsEnabled)
		}

		rows, err := tx.QueryContext(txCtx, `
			UPDATE entities e
			SET is_enabled = u.is_enabled, updated_at = NOW()
			FROM spaces s,
			     unnest($1::uuid[], $2::boolean[]) AS u(id, is_enabled)
			WHERE e.id = u.id
			  AND e.space_id = s.space_id
			  AND s.space_slug = $3
			RETURNING e.id, e.is_enabled, e.updated_at
		`, pq.Array(entityIDs), pq.Array(enabledStates), spaceSlug)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()

		updatedEntities := make([]BulkUpdateEntityResult, 0, len(updates))
		for rows.Next() {
			var updatedID uuid.UUID
			var isEnabled bool
			var updatedAt sql.NullTime
			if err := rows.Scan(&updatedID, &isEnabled, &updatedAt); err != nil {
				return err
			}
			updatedEntities = append(updatedEntities, BulkUpdateEntityResult{
				EntityID:  updatedID.String(),
				IsEnabled: isEnabled,
				UpdatedAt: updatedAt.Time,
			})
		}
		if err := rows.Err(); err != nil {
			return err
		}

		result = &BulkUpdateEntitiesResult{
			Entities:     updatedEntities,
			UpdatedCount: len(updatedEntities),
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (c *Client) UpdateEntitiesBySelection(ctx context.Context, selectedIDs []uuid.UUID, spaceSlug string, isEnabled bool) (*BulkUpdateEntitiesResult, error) {
	org := orgFromContext(ctx)
	if org == "" {
		return nil, fmt.Errorf("organization not found in context")
	}
	if strings.TrimSpace(spaceSlug) == "" {
		return nil, fmt.Errorf("space_slug is required")
	}

	var result *BulkUpdateEntitiesResult
	err := c.WithOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
		rows, err := tx.QueryContext(txCtx, `
			UPDATE entities e
			SET is_enabled = CASE WHEN e.id = ANY($1::uuid[]) THEN NOT $3 ELSE $3 END,
			    updated_at = NOW()
			FROM spaces s
			WHERE e.space_id = s.space_id
			  AND s.space_slug = $2
			RETURNING e.id, e.is_enabled, e.updated_at
		`, pq.Array(selectedIDs), spaceSlug, isEnabled)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()

		updatedEntities := make([]BulkUpdateEntityResult, 0)
		for rows.Next() {
			var updatedID uuid.UUID
			var isEnabled bool
			var updatedAt sql.NullTime
			if err := rows.Scan(&updatedID, &isEnabled, &updatedAt); err != nil {
				return err
			}
			updatedEntities = append(updatedEntities, BulkUpdateEntityResult{
				EntityID:  updatedID.String(),
				IsEnabled: isEnabled,
				UpdatedAt: updatedAt.Time,
			})
		}
		if err := rows.Err(); err != nil {
			return err
		}

		result = &BulkUpdateEntitiesResult{
			Entities:     updatedEntities,
			UpdatedCount: len(updatedEntities),
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return result, nil
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

	normalizedDevEUI := strings.ToUpper(strings.TrimSpace(devEUI))

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

			entityUniqueKey := fmt.Sprintf("%s_%s_%s", modelKey, normalizedDevEUI, templateKey)

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
				name, manufacturer, unit_of_measurement, display_type, icon, is_enabled, created_at, updated_at
			)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, true, now(), now())
			ON CONFLICT (unique_key) DO NOTHING
		`,
			uuid.New(),
			spaceID,
			deviceUUID,
			entityUniqueKey,
			tpl.Category,
			entityTypeID,
			tpl.Name,
			tpl.Manufacturer,
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
