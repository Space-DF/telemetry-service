package timescaledb

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/Space-DF/telemetry-service/internal/models"
	"github.com/google/uuid"
	"github.com/stephenafamo/bob"
)

// GetGeofences retrieves geofences with optional filters and pagination
func (c *Client) GetGeofences(ctx context.Context, spaceID *uuid.UUID, isActive *bool, search string, page, pageSize int) ([]models.GeofenceWithSpace, int, error) {
	if page <= 0 {
		page = DefaultPage
	}
	if pageSize <= 0 || pageSize > MaxPageSize {
		pageSize = DefaultPageSize
	}

	offset := (page - 1) * pageSize

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

		// Count total
		countQuery := `SELECT COUNT(*) FROM geofences g ` + whereClause
		err := tx.QueryRowContext(txCtx, countQuery, args...).Scan(&total)
		if err != nil {
			return fmt.Errorf("failed to count geofences: %w", err)
		}

		// Query geofences with space join
		args = append(args, pageSize, offset)
		query := fmt.Sprintf(`
			SELECT g.geofence_id, g.name, g.type_zone, ST_AsGeoJSON(g.geometry)::json,
				   g.is_active, g.space_id, g.created_at, g.updated_at,
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
			var geometryJSON []byte
			var spaceName, spaceSlug, spaceLogo sql.NullString

			err := rows.Scan(
				&g.GeofenceID, &g.Name, &g.TypeZone, &geometryJSON,
				&g.IsActive, &g.SpaceID, &g.CreatedAt, &g.UpdatedAt,
				&spaceName, &spaceSlug, &spaceLogo,
			)
			if err != nil {
				return err
			}

			g.Geometry = models.Geometry(geometryJSON)

			if spaceName.Valid {
				g.SpaceName = &spaceName.String
			}
			if spaceSlug.Valid {
				g.SpaceSlug = &spaceSlug.String
			}
			if spaceLogo.Valid {
				g.SpaceLogo = &spaceLogo.String
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
		var geometryJSON []byte
		var spaceName, spaceSlug, spaceLogo sql.NullString

		query := `
			SELECT g.geofence_id, g.name, g.type_zone, ST_AsGeoJSON(g.geometry)::json,
				   g.is_active, g.space_id, g.created_at, g.updated_at,
				   s.name as space_name, s.space_slug as space_slug, s.logo as space_logo
			FROM geofences g
			LEFT JOIN spaces s ON g.space_id = s.space_id
			WHERE g.geofence_id = $1
		`

		err := tx.QueryRowContext(txCtx, query, geofenceID).Scan(
			&g.GeofenceID, &g.Name, &g.TypeZone, &geometryJSON,
			&g.IsActive, &g.SpaceID, &g.CreatedAt, &g.UpdatedAt,
			&spaceName, &spaceSlug, &spaceLogo,
		)
		if err != nil {
			if err == sql.ErrNoRows {
				return models.ErrGeofenceNotFound
			}
			return fmt.Errorf("failed to query geofence: %w", err)
		}

		g.Geometry = models.Geometry(geometryJSON)

		if spaceName.Valid {
			g.SpaceName = &spaceName.String
		}
		if spaceSlug.Valid {
			g.SpaceSlug = &spaceSlug.String
		}
		if spaceLogo.Valid {
			g.SpaceLogo = &spaceLogo.String
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return &g, nil
}

// CreateGeofence creates a new geofence
func (c *Client) CreateGeofence(ctx context.Context, name, typeZone string, geometry []byte, spaceID *uuid.UUID, isActive *bool) (*models.Geofence, error) {
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
		// Insert geofence with geometry from GeoJSON
		query := `
			INSERT INTO geofences (name, type_zone, geometry, is_active, space_id)
			VALUES ($1, $2, ST_GeomFromGeoJSON($3), $4, $5)
			RETURNING geofence_id, created_at, updated_at
		`

		err := tx.QueryRowContext(txCtx, query,
			name, typeZone, geometry, isActiveVal, spaceID).Scan(
			&g.GeofenceID, &g.CreatedAt, &g.UpdatedAt,
		)
		if err != nil {
			return fmt.Errorf("failed to insert geofence: %w", err)
		}

		g.Name = name
		g.TypeZone = typeZone
		g.Geometry = models.Geometry(geometry)
		g.IsActive = isActiveVal
		g.SpaceID = spaceID

		return nil
	})

	if err != nil {
		return nil, err
	}

	return &g, nil
}

// UpdateGeofence updates an existing geofence
func (c *Client) UpdateGeofence(ctx context.Context, geofenceID uuid.UUID, name, typeZone *string, geometry []byte, spaceID *uuid.UUID, isActive *bool) (*models.Geofence, error) {
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
		var currentName, currentTypeZone string
		var currentIsActive bool
		var currentSpaceID sql.NullString

		getQuery := `
			SELECT name, type_zone, ST_AsGeoJSON(geometry), is_active, space_id
			FROM geofences WHERE geofence_id = $1
		`
		err := tx.QueryRowContext(txCtx, getQuery, geofenceID).Scan(
			&currentName, &currentTypeZone, &currentGeometry, &currentIsActive, &currentSpaceID,
		)
		if err != nil {
			if err == sql.ErrNoRows {
				return models.ErrGeofenceNotFound
			}
			return fmt.Errorf("failed to get geofence: %w", err)
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

		// If no fields to update, return current geofence
		if len(setClauses) == 0 {
			g.GeofenceID = geofenceID
			g.Name = currentName
			g.TypeZone = currentTypeZone
			g.Geometry = models.Geometry(currentGeometry)
			g.IsActive = currentIsActive
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
			RETURNING geofence_id, name, type_zone, ST_AsGeoJSON(geometry)::json, is_active, space_id, created_at, updated_at
		`, strings.Join(setClauses, ", "), argIdx)

		var geometryJSON []byte
		err = tx.QueryRowContext(txCtx, query, args...).Scan(
			&g.GeofenceID, &g.Name, &g.TypeZone, &geometryJSON,
			&g.IsActive, &g.SpaceID, &g.CreatedAt, &g.UpdatedAt,
		)
		if err != nil {
			if err == sql.ErrNoRows {
				return models.ErrGeofenceNotFound
			}
			return fmt.Errorf("failed to update geofence: %w", err)
		}

		g.Geometry = models.Geometry(geometryJSON)
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
		result, err := tx.ExecContext(txCtx, `DELETE FROM geofences WHERE geofence_id = $1`, geofenceID)
		if err != nil {
			return fmt.Errorf("failed to delete geofence: %w", err)
		}

		rows, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("failed to get rows affected: %w", err)
		}
		if rows == 0 {
			return models.ErrGeofenceNotFound
		}

		return nil
	})
}

// GetGeofencesByDevice retrieves geofences associated with a device through event_rules
func (c *Client) GetGeofencesByDevice(ctx context.Context, deviceID string) ([]models.GeofenceWithSpace, error) {
	if deviceID == "" {
		return nil, fmt.Errorf("device_id is required")
	}

	var geofences []models.GeofenceWithSpace

	org := orgFromContext(ctx)
	if org == "" {
		return nil, fmt.Errorf("organization not found in context")
	}

	err := c.WithOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
		// Query geofences that are referenced by event_rules for this device
		query := `
			SELECT DISTINCT g.geofence_id, g.name, g.type_zone, ST_AsGeoJSON(g.geometry)::json,
				g.is_active, g.space_id, g.created_at, g.updated_at,
				s.name as space_name, s.space_slug as space_slug, s.logo as space_logo
			FROM geofences g
			INNER JOIN event_rules er ON er.geofence_id = g.geofence_id
			LEFT JOIN spaces s ON g.space_id = s.space_id
			WHERE er.device_id = $1 AND g.is_active = true
			ORDER BY g.name
		`

		rows, err := tx.QueryContext(txCtx, query, deviceID)
		if err != nil {
			return fmt.Errorf("failed to query geofences by device: %w", err)
		}
		defer func() { _ = rows.Close() }()

		for rows.Next() {
			var g models.GeofenceWithSpace
			var geometryJSON []byte
			var spaceName, spaceSlug, spaceLogo sql.NullString

			err := rows.Scan(
				&g.GeofenceID, &g.Name, &g.TypeZone, &geometryJSON,
				&g.IsActive, &g.SpaceID, &g.CreatedAt, &g.UpdatedAt,
				&spaceName, &spaceSlug, &spaceLogo,
			)
			if err != nil {
				return err
			}

			g.Geometry = models.Geometry(geometryJSON)

			if spaceName.Valid {
				g.SpaceName = &spaceName.String
			}
			if spaceSlug.Valid {
				g.SpaceSlug = &spaceSlug.String
			}
			if spaceLogo.Valid {
				g.SpaceLogo = &spaceLogo.String
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
