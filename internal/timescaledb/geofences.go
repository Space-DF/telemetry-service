package timescaledb

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/Space-DF/telemetry-service/internal/api/common"
	"github.com/Space-DF/telemetry-service/internal/models"
	"github.com/google/uuid"
	"github.com/stephenafamo/bob"
)

// GetGeofences retrieves geofences with optional filters and pagination
func (c *Client) GetGeofences(ctx context.Context, spaceID *uuid.UUID, isActive *bool, search string, bboxEnvelope *[4]float64, limit, offset int) ([]models.GeofenceWithSpace, int, error) {
	if limit <= 0 {
		limit = common.DefaultLimit
	}
	if offset < 0 {
		offset = 0
	}

	var geofences []models.GeofenceWithSpace
	var total int

	org := orgFromContext(ctx)
	if org == "" {
		return nil, 0, fmt.Errorf("organization not found in context")
	}

	err := c.WithOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
		// Build where clause
		whereClause := "WHERE 1=1"
		args := []interface{}{}
		argIdx := 1

		if spaceID != nil {
			whereClause += fmt.Sprintf(" AND g.space_id = $%d", argIdx)
			args = append(args, *spaceID)
			argIdx++
		}
		if isActive != nil {
			whereClause += fmt.Sprintf(" AND g.is_active = $%d", argIdx)
			args = append(args, *isActive)
			argIdx++
		}
		if search != "" {
			whereClause += fmt.Sprintf(" AND g.name ILIKE $%d", argIdx)
			args = append(args, "%"+search+"%")
			argIdx++
		}

		// Add spatial bbox filter if provided
		if bboxEnvelope != nil {
			whereClause += fmt.Sprintf(
				" AND ST_Intersects(g.geometry, ST_MakeEnvelope($%d,$%d,$%d,$%d,4326))",
				argIdx, argIdx+1, argIdx+2, argIdx+3,
			)
			args = append(args,
				bboxEnvelope[0],
				bboxEnvelope[1],
				bboxEnvelope[2],
				bboxEnvelope[3],
			)
			argIdx += 4
		}

		// Count total
		countQuery := `SELECT COUNT(*) FROM geofences g ` + whereClause
		err := tx.QueryRowContext(txCtx, countQuery, args...).Scan(&total)
		if err != nil {
			return fmt.Errorf("failed to count geofences: %w", err)
		}

		// Query geofences with space join
		args = append(args, limit, offset)
		query := fmt.Sprintf(`
			SELECT g.geofence_id, g.name, g.type_zone, g.features, g.color,
				g.is_active, g.space_id, g.event_rule_id, g.created_at, g.updated_at,
				s.name as space_name, s.space_slug as space_slug, s.logo as space_logo
			FROM geofences g
			LEFT JOIN spaces s ON g.space_id = s.space_id
			%s
			ORDER BY g.created_at DESC
			LIMIT $%d OFFSET $%d
		`, whereClause, argIdx, argIdx+1)

		rows, err := tx.QueryContext(txCtx, query, args...)
		if err != nil {
			return fmt.Errorf("failed to query geofences: %w", err)
		}
		defer func() { _ = rows.Close() }()

		for rows.Next() {
			var g models.GeofenceWithSpace
			var featuresJSON []byte
			var color sql.NullString
			var eventRuleID sql.NullString
			var spaceName, spaceSlug, spaceLogo sql.NullString

			err := rows.Scan(
				&g.GeofenceID, &g.Name, &g.TypeZone, &featuresJSON, &color,
				&g.IsActive, &g.SpaceID, &eventRuleID, &g.CreatedAt, &g.UpdatedAt,
				&spaceName, &spaceSlug, &spaceLogo,
			)
			if err != nil {
				return err
			}

			if len(featuresJSON) > 0 {
				if err := json.Unmarshal(featuresJSON, &g.Features); err != nil {
					return fmt.Errorf("failed to unmarshal features JSON: %w", err)
				}
			}
			if color.Valid {
				g.Color = color.String
			}
			if eventRuleID.Valid {
				eventRuleUUID, err := uuid.Parse(eventRuleID.String)
				if err == nil {
					g.EventRuleID = &eventRuleUUID
				}
			}

			geofences = append(geofences, g)
		}

		return rows.Err()
	})

	if err != nil {
		return nil, 0, err
	}

	return geofences, total, nil
}

// GetGeofenceByID retrieves a single geofence by ID
func (c *Client) GetGeofenceByID(ctx context.Context, geofenceID uuid.UUID) (*models.GeofenceWithSpace, error) {
	if geofenceID == uuid.Nil {
		return nil, fmt.Errorf("geofence_id is required")
	}

	var g models.GeofenceWithSpace

	org := orgFromContext(ctx)
	if org == "" {
		return nil, fmt.Errorf("organization not found in context")
	}

	err := c.WithOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
		var featuresJSON []byte
		var color sql.NullString
		var eventRuleID sql.NullString
		var spaceName, spaceSlug, spaceLogo sql.NullString

		query := `
			SELECT g.geofence_id, g.name, g.type_zone, g.features, g.color,
				g.is_active, g.space_id, g.event_rule_id, g.created_at, g.updated_at,
				s.name as space_name, s.space_slug as space_slug, s.logo as space_logo
			FROM geofences g
			LEFT JOIN spaces s ON g.space_id = s.space_id
			WHERE g.geofence_id = $1
		`

		err := tx.QueryRowContext(txCtx, query, geofenceID).Scan(
			&g.GeofenceID, &g.Name, &g.TypeZone, &featuresJSON, &color,
			&g.IsActive, &g.SpaceID, &eventRuleID, &g.CreatedAt, &g.UpdatedAt,
			&spaceName, &spaceSlug, &spaceLogo,
		)
		if err != nil {
			if err == sql.ErrNoRows {
				return models.ErrGeofenceNotFound
			}
			return fmt.Errorf("failed to query geofence: %w", err)
		}

		if len(featuresJSON) > 0 {
			if err := json.Unmarshal(featuresJSON, &g.Features); err != nil {
				return fmt.Errorf("failed to unmarshal features JSON: %w", err)
			}
		}
		if color.Valid {
			g.Color = color.String
		}
		if eventRuleID.Valid {
			eventRuleUUID, err := uuid.Parse(eventRuleID.String)
			if err == nil {
				g.EventRuleID = &eventRuleUUID
			}
		}

		return nil
	})
	if err != nil {
		return nil, err
	}
	return &g, nil
}

// CreateGeofence creates a new geofence with an associated event rule
func (c *Client) CreateGeofence(ctx context.Context, name, typeZone string, geometry []byte, features []json.RawMessage, color string, spaceID *uuid.UUID, isActive *bool, eventRuleID *uuid.UUID, definition *json.RawMessage) (*models.Geofence, error) {
	org := orgFromContext(ctx)
	if org == "" {
		return nil, fmt.Errorf("organization not found in context")
	}

	var g models.Geofence
	isActiveVal := true
	if isActive != nil {
		isActiveVal = *isActive
	}

	err := c.WithOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
		// First create the event rule if not provided
		var ruleID uuid.UUID
		if eventRuleID != nil {
			ruleID = *eventRuleID
		} else {
			// Create default event rule for geofence
			isActive := true
			repeatAble := true
			cooldownSec := 0
			ruleKey := "geofence"
			description := name
			definitionStr := "{}"
			if definition != nil && len(*definition) > 0 {
				definitionStr = string(*definition)
			}

			err := tx.QueryRowContext(txCtx, `
				INSERT INTO event_rules (rule_key, definition, is_active, repeat_able, cooldown_sec, description)
				VALUES ($1, $2::jsonb, $3, $4, $5, $6)
				RETURNING event_rule_id
			`, ruleKey, definitionStr, isActive, repeatAble, cooldownSec, description).Scan(&ruleID)
			if err != nil {
				return fmt.Errorf("failed to create event rule: %w", err)
			}
		}

		// Insert geofence with geometry from GeoJSON
		featuresJSON, err := json.Marshal(features)
		if err != nil {
			return fmt.Errorf("failed to marshal features to JSON: %w", err)
		}

		query := `
			INSERT INTO geofences (name, type_zone, geometry, features, color, is_active, space_id, event_rule_id)
			VALUES ($1, $2, ST_GeomFromGeoJSON($3), $4, $5, $6, $7, $8)
			RETURNING geofence_id, created_at, updated_at
		`

		err = tx.QueryRowContext(txCtx, query,
			name, typeZone, geometry, featuresJSON, color, isActiveVal, spaceID, ruleID).Scan(
			&g.GeofenceID, &g.CreatedAt, &g.UpdatedAt,
		)
		if err != nil {
			return fmt.Errorf("failed to insert geofence: %w", err)
		}

		g.Name = name
		g.TypeZone = typeZone
		g.Features = features
		g.Color = color
		g.IsActive = isActiveVal
		g.SpaceID = spaceID
		g.EventRuleID = &ruleID

		return nil
	})

	if err != nil {
		return nil, err
	}

	return &g, nil
}

// UpdateGeofence updates an existing geofence
func (c *Client) UpdateGeofence(ctx context.Context, geofenceID uuid.UUID, name, typeZone *string, geometry []byte, features []json.RawMessage, color *string, spaceID *uuid.UUID, isActive *bool, eventRuleID *uuid.UUID) (*models.Geofence, error) {
	if geofenceID == uuid.Nil {
		return nil, fmt.Errorf("geofence_id is required")
	}

	org := orgFromContext(ctx)
	if org == "" {
		return nil, fmt.Errorf("organization not found in context")
	}

	var g models.Geofence

	err := c.WithOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
		// First get current geofence
		var currentGeometry []byte
		var currentFeatures []json.RawMessage
		var currentColor string
		var currentName, currentTypeZone string
		var currentIsActive bool
		var currentSpaceID sql.NullString
		var currentEventRuleID sql.NullString

		getQuery := `
			SELECT name, type_zone, ST_AsGeoJSON(geometry), features, color, is_active, space_id, event_rule_id
			FROM geofences WHERE geofence_id = $1
		`
		var featuresRaw []byte
		err := tx.QueryRowContext(txCtx, getQuery, geofenceID).Scan(
			&currentName, &currentTypeZone, &currentGeometry, &featuresRaw, &currentColor, &currentIsActive, &currentSpaceID, &currentEventRuleID,
		)
		if err != nil {
			if err == sql.ErrNoRows {
				return models.ErrGeofenceNotFound
			}
			return fmt.Errorf("failed to get geofence: %w", err)
		}
		if err := json.Unmarshal(featuresRaw, &currentFeatures); err != nil {
			return fmt.Errorf("failed to unmarshal current features JSON: %w", err)
		}

		// Use current values if not provided in update request
		var setClauses []string
		var args []interface{}
		argIdx := 1

		if name != nil {
			setClauses = append(setClauses, fmt.Sprintf("name = $%d", argIdx))
			args = append(args, *name)
			argIdx++
		}
		if typeZone != nil {
			setClauses = append(setClauses, fmt.Sprintf("type_zone = $%d", argIdx))
			args = append(args, *typeZone)
			argIdx++
		}
		if geometry != nil {
			setClauses = append(setClauses, fmt.Sprintf("geometry = ST_GeomFromGeoJSON($%d)", argIdx))
			args = append(args, geometry)
			argIdx++
		}
		if isActive != nil {
			setClauses = append(setClauses, fmt.Sprintf("is_active = $%d", argIdx))
			args = append(args, *isActive)
			argIdx++
		}
		if spaceID != nil {
			if *spaceID == uuid.Nil {
				setClauses = append(setClauses, fmt.Sprintf("space_id = $%d", argIdx))
				args = append(args, nil)
			} else {
				setClauses = append(setClauses, fmt.Sprintf("space_id = $%d", argIdx))
				args = append(args, *spaceID)
			}
			argIdx++
		}
		if color != nil {
			setClauses = append(setClauses, fmt.Sprintf("color = $%d", argIdx))
			args = append(args, *color)
			argIdx++
		}
		if features != nil {
			featuresJSON, err := json.Marshal(features)
			if err != nil {
				return fmt.Errorf("failed to marshal features JSON: %w", err)
			}
			setClauses = append(setClauses, fmt.Sprintf("features = $%d", argIdx))
			args = append(args, featuresJSON)
			argIdx++
		}
		if eventRuleID != nil {
			setClauses = append(setClauses, fmt.Sprintf("event_rule_id = $%d", argIdx))
			args = append(args, *eventRuleID)
			argIdx++
		}

		// If no fields to update, return current geofence
		if len(setClauses) == 0 {
			g.GeofenceID = geofenceID
			g.Name = currentName
			g.TypeZone = currentTypeZone
			g.IsActive = currentIsActive
			g.Color = currentColor
			if currentEventRuleID.Valid {
				eventRuleUUID, err := uuid.Parse(currentEventRuleID.String)
				if err == nil {
					g.EventRuleID = &eventRuleUUID
				}
			}
			if currentSpaceID.Valid {
				spaceUUID, err := uuid.Parse(currentSpaceID.String)
				if err != nil {
					return fmt.Errorf("failed to parse current space ID %q as UUID: %w", currentSpaceID.String, err)
				}
				g.SpaceID = &spaceUUID
			}
			return nil
		}

		args = append(args, geofenceID)

		query := fmt.Sprintf(`
			UPDATE geofences
			SET %s
			WHERE geofence_id = $%d
			RETURNING geofence_id, name, type_zone, ST_AsGeoJSON(geometry)::json, features, color, is_active, space_id, event_rule_id, created_at, updated_at
		`, strings.Join(setClauses, ", "), argIdx)

		var geometryJSON []byte
		var featuresJSON []byte
		var eventRuleID sql.NullString
		err = tx.QueryRowContext(txCtx, query, args...).Scan(
			&g.GeofenceID, &g.Name, &g.TypeZone, &geometryJSON, &featuresJSON, &g.Color,
			&g.IsActive, &g.SpaceID, &eventRuleID, &g.CreatedAt, &g.UpdatedAt,
		)
		if err != nil {
			if err == sql.ErrNoRows {
				return models.ErrGeofenceNotFound
			}
			return fmt.Errorf("failed to update geofence: %w", err)
		}

		if len(featuresJSON) > 0 {
			if err := json.Unmarshal(featuresJSON, &g.Features); err != nil {
				return fmt.Errorf("failed to unmarshal features JSON: %w", err)
			}
		}
		if eventRuleID.Valid {
			eventRuleUUID, err := uuid.Parse(eventRuleID.String)
			if err == nil {
				g.EventRuleID = &eventRuleUUID
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return &g, nil
}

// DeleteGeofence deletes a geofence
func (c *Client) DeleteGeofence(ctx context.Context, geofenceID uuid.UUID) error {
	if geofenceID == uuid.Nil {
		return fmt.Errorf("geofence_id is required")
	}

	org := orgFromContext(ctx)
	if org == "" {
		return fmt.Errorf("organization not found in context")
	}

	return c.WithOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
		var eventRuleID string

		// Get associated event rule ID to delete after deleting geofence
		err := tx.QueryRowContext(txCtx, `DELETE FROM geofences WHERE geofence_id = $1 RETURNING event_rule_id`, geofenceID).Scan(&eventRuleID)

		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("geofence not found")
			}
			return fmt.Errorf("failed to delete geofence: %w", err)
		}

		// Delete associated event rule
		_, err = tx.ExecContext(txCtx, `
			DELETE FROM event_rules WHERE event_rule_id = $1
		`, eventRuleID)
		if err != nil {
			return fmt.Errorf("failed to delete associated event rule: %w", err)
		}

		return nil
	})
}

// GetGeofencesBySpace retrieves geofences associated with a space
func (c *Client) GetGeofencesBySpace(ctx context.Context, spaceID uuid.UUID) ([]models.GeofenceWithSpace, error) {
	if spaceID == uuid.Nil {
		return nil, fmt.Errorf("space_id is required")
	}

	var geofences []models.GeofenceWithSpace

	org := orgFromContext(ctx)
	if org == "" {
		return nil, fmt.Errorf("organization not found in context")
	}

	err := c.WithOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
		// Query geofences in the given space
		query := `
			SELECT g.geofence_id, g.name, g.type_zone, g.features, g.color,
				g.is_active, g.space_id, g.event_rule_id, g.created_at, g.updated_at,
				sp.name as space_name, sp.space_slug as space_slug, sp.logo as space_logo
			FROM geofences g
			LEFT JOIN spaces sp ON g.space_id = sp.space_id
			WHERE g.space_id = $1
			AND g.is_active = true
			ORDER BY g.name
		`

		rows, err := tx.QueryContext(txCtx, query, spaceID)
		if err != nil {
			return fmt.Errorf("failed to query geofences by device: %w", err)
		}
		defer func() { _ = rows.Close() }()

		for rows.Next() {
			var g models.GeofenceWithSpace
			var featuresJSON []byte
			var color sql.NullString
			var eventRuleID sql.NullString
			var spaceName, spaceSlug, spaceLogo sql.NullString

			err := rows.Scan(
				&g.GeofenceID, &g.Name, &g.TypeZone, &featuresJSON, &color,
				&g.IsActive, &g.SpaceID, &eventRuleID, &g.CreatedAt, &g.UpdatedAt,
				&spaceName, &spaceSlug, &spaceLogo,
			)
			if err != nil {
				return err
			}

			if len(featuresJSON) > 0 {
				if err := json.Unmarshal(featuresJSON, &g.Features); err != nil {
					return fmt.Errorf("failed to unmarshal features JSON: %w", err)
				}
			}
			if color.Valid {
				g.Color = color.String
			}
			if eventRuleID.Valid {
				eventRuleUUID, err := uuid.Parse(eventRuleID.String)
				if err == nil {
					g.EventRuleID = &eventRuleUUID
				}
			}

			geofences = append(geofences, g)
		}

		return rows.Err()
	})

	if err != nil {
		return nil, err
	}

	return geofences, nil
}

// IsPointInGeofence checks whether a lat/lon coordinate lies inside the given geofence.
func (c *Client) IsPointInGeofence(ctx context.Context, geofenceID string, lat, lon float64) (isInside bool, typeZone string, err error) {
	org := orgFromContext(ctx)
	if org == "" {
		return false, "", fmt.Errorf("organization not found in context")
	}

	err = c.WithOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
		return tx.QueryRowContext(txCtx, `
			SELECT type_zone,
			       ST_Contains(geometry, ST_SetSRID(ST_MakePoint($2, $3), 4326))
			FROM geofences
			WHERE geofence_id::text = $1 AND is_active = true
		`, geofenceID, lon, lat).Scan(&typeZone, &isInside)
	})

	if err != nil {
		return false, "", err
	}

	return isInside, typeZone, nil
}

// DistanceToGeofenceKm returns the distance in kilometers from a point to the nearest
// edge of the given geofence. Returns 0 if the point is inside the geofence.
func (c *Client) DistanceToGeofenceKm(ctx context.Context, geofenceID string, lat, lon float64) (float64, error) {
	org := orgFromContext(ctx)
	if org == "" {
		return 0, fmt.Errorf("organization not found in context")
	}

	var distanceKm float64
	err := c.WithOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
		return tx.QueryRowContext(txCtx, `
			SELECT ST_Distance(
				geometry::geography,
				ST_SetSRID(ST_MakePoint($2, $3), 4326)::geography
			) / 1000.0
			FROM geofences
			WHERE geofence_id::text = $1 AND is_active = true
		`, geofenceID, lon, lat).Scan(&distanceKm)
	})

	if err != nil {
		return 0, err
	}

	return distanceKm, nil
}

// TestGeofenceWithGeometry checks all devices' last locations in a space against a provided geofence geometry
// in a single PostGIS query. Returns is_inside and distance_km for each device.
func (c *Client) TestGeofenceWithGeometry(ctx context.Context, spaceID string, geometry string) ([]models.DeviceGeofenceCheck, error) {
	org := orgFromContext(ctx)
	if org == "" {
		return nil, fmt.Errorf("organization not found in context")
	}

	var results []models.DeviceGeofenceCheck
	err := c.WithOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
		query := `
			WITH last_locations AS (
				SELECT DISTINCT ON (e.device_id)
					e.device_id::text AS device_id,
					(a.shared_attrs->>'latitude')::float8  AS lat,
					(a.shared_attrs->>'longitude')::float8 AS lon,
					s.reported_at
				FROM entity_states s
				JOIN entities e ON s.entity_id = e.id
				JOIN spaces sp ON e.space_id = sp.space_id
				LEFT JOIN entity_state_attributes a ON s.attributes_id = a.id
				WHERE sp.space_id::text = $1
					AND e.category = 'location'
					AND a.shared_attrs IS NOT NULL
					AND a.shared_attrs ? 'latitude'
					AND a.shared_attrs ? 'longitude'
				ORDER BY e.device_id, s.reported_at DESC
			)
			SELECT
				ll.device_id,
				ll.lat,
				ll.lon,
				ll.reported_at,
				ST_Contains(ST_GeomFromGeoJSON($2), ST_SetSRID(ST_MakePoint(ll.lon, ll.lat), 4326)) AS is_inside,
				ST_Distance(
					ST_SetSRID(ST_MakePoint(ll.lon, ll.lat), 4326)::geography,
					ST_GeomFromGeoJSON($2)::geography
				) / 1000.0 AS distance_km
			FROM last_locations ll`

		rows, err := tx.QueryContext(txCtx, query, spaceID, geometry)
		if err != nil {
			return fmt.Errorf("failed to test geofence with geometry: %w", err)
		}
		defer func() { _ = rows.Close() }()

		for rows.Next() {
			var r models.DeviceGeofenceCheck
			if err := rows.Scan(&r.DeviceID, &r.Latitude, &r.Longitude, &r.ReportedAt, &r.IsInside, &r.DistanceKm); err != nil {
				return err
			}
			results = append(results, r)
		}
		return rows.Err()
	})

	if err != nil {
		return nil, err
	}

	return results, nil
}
