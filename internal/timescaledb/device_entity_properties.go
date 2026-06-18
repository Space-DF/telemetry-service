package timescaledb

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/lib/pq"
	"github.com/stephenafamo/bob"
)

func parseEntityValue(raw sql.NullString) interface{} {
	if !raw.Valid {
		return nil
	}

	if parsedFloat, err := strconv.ParseFloat(raw.String, 64); err == nil {
		return parsedFloat
	}

	if raw.String == "true" {
		return true
	}
	if raw.String == "false" {
		return false
	}

	return raw.String
}

func normalizeLocationValue(rawAttrs []byte) interface{} {
	if len(rawAttrs) == 0 {
		return nil
	}

	var attrs map[string]interface{}
	if err := json.Unmarshal(rawAttrs, &attrs); err != nil {
		return nil
	}

	lat, hasLat := attrs["latitude"]
	lon, hasLon := attrs["longitude"]
	if !hasLat || !hasLon {
		return nil
	}

	value := map[string]interface{}{
		"latitude":  lat,
		"longitude": lon,
	}
	if bearing, ok := attrs["bearing"]; ok {
		value["bearing"] = bearing
	}
	return value
}

// GetDeviceEntityProperties returns device entities from a single DB query.
func (c *Client) GetDeviceEntityProperties(ctx context.Context, deviceID string) ([]map[string]interface{}, error) {
	org := orgFromContext(ctx)
	if org == "" {
		return nil, fmt.Errorf("organization not found in context")
	}

	result := make([]map[string]interface{}, 0)

	query := `
		SELECT
			e.id,
			e.device_id::text,
			e.name,
			e.manufacturer,
			e.unique_key,
			et.id AS entity_type_id,
			et.name AS entity_type_name,
			et.unique_key AS entity_type_unique_key,
			et.image_url AS entity_type_image_url,
			e.category,
			e.unit_of_measurement,
			e.display_type,
			e.icon,
			e.is_enabled,
			e.created_at,
			e.updated_at,
			state_range.time_start,
			state_range.time_end,
			latest_state.state,
			latest_state.reported_at,
			COALESCE(state_attrs.shared_attrs, '{}'::jsonb)
		FROM entities e
		LEFT JOIN entity_types et ON e.entity_type_id = et.id
		LEFT JOIN LATERAL (
			SELECT
				MIN(es.reported_at) AS time_start,
				MAX(es.reported_at) AS time_end
			FROM entity_states es
			WHERE es.entity_id = e.id
		) state_range ON true
		LEFT JOIN LATERAL (
			SELECT
				es.state,
				es.reported_at,
				es.attributes_id
			FROM entity_states es
			WHERE es.entity_id = e.id
			ORDER BY es.reported_at DESC
			LIMIT 1
		) latest_state ON true
		LEFT JOIN entity_state_attributes state_attrs ON latest_state.attributes_id = state_attrs.id
		WHERE e.device_id::text = $1
		ORDER BY e.created_at DESC
	`

	err := c.WithOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
		rows, err := tx.QueryContext(txCtx, query, deviceID)
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
			var latestState sql.NullString
			var latestReportedAt sql.NullTime
			var rawAttrs []byte

			if err := rows.Scan(
				&id,
				&deviceIDCol,
				&name,
				&manufacturer,
				&uniqueKey,
				&etID,
				&etName,
				&etUnique,
				&etImage,
				&categoryCol,
				&unit,
				&displayType,
				&icon,
				&isEnabled,
				&createdAt,
				&updatedAt,
				&timeStart,
				&timeEnd,
				&latestState,
				&latestReportedAt,
				&rawAttrs,
			); err != nil {
				return err
			}

			entityValue := parseEntityValue(latestState)
			if categoryCol.String == "location" {
				if locationValue := normalizeLocationValue(rawAttrs); locationValue != nil {
					entityValue = locationValue
				}
			}

			entityRow := buildEntityRowMap(
				c,
				id,
				deviceIDCol,
				name,
				manufacturer,
				uniqueKey,
				etID,
				etName,
				etUnique,
				etImage,
				categoryCol,
				unit,
				icon,
				displayType,
				isEnabled,
				createdAt,
				updatedAt,
				timeStart,
				timeEnd,
			)
			entityRow["value"] = entityValue
			result = append(result, entityRow)
		}

		return rows.Err()
	})
	if err != nil {
		return nil, fmt.Errorf("failed to query device entity properties: %w", err)
	}

	return result, nil
}
