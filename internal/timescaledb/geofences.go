package timescaledb

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/Space-DF/telemetry-service/internal/models"
	"github.com/google/uuid"
	"github.com/stephenafamo/bob"
)

// GetGeofences retrieves geofences with optional filters and pagination
func (c *Client) GetGeofences(ctx context.Context, spaceID *uuid.UUID, isActive *bool, page, pageSize int) ([]models.GeofenceWithSpace, int, error) {
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
				return fmt.Errorf("geofence not found")
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
	isActiveVal := false
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
			SELECT name, type_zone, ST_AsGeoJSON(geometry), is_active, space_id,
			FROM geofences WHERE geofence_id = $1
		`
		err := tx.QueryRowContext(txCtx, getQuery, geofenceID).Scan(
			&currentName, &currentTypeZone, &currentGeometry, &currentIsActive, &currentSpaceID,
		)
		if err != nil {
			if err == sql.ErrNoRows {
				return fmt.Errorf("geofence not found")
			}
			return fmt.Errorf("failed to get geofence: %w", err)
		}

		// Use current values if not provided in update request
		nameVal := currentName
		typeZoneVal := currentTypeZone
		geometryVal := currentGeometry
		isActiveVal := currentIsActive
		spaceIDVal := currentSpaceID

		if name != nil {
			nameVal = *name
		}
		if typeZone != nil {
			typeZoneVal = *typeZone
		}
		if geometry != nil {
			geometryVal = geometry
		}
		if isActive != nil {
			isActiveVal = *isActive
		}
		if spaceID != nil {
			if *spaceID == uuid.Nil {
				spaceIDVal = sql.NullString{Valid: false}
			} else {
				spaceIDVal = sql.NullString{String: spaceID.String(), Valid: true}
			}
		}

		// Update geofence
		updateQuery := `
			UPDATE geofences
			SET name = $1, type_zone = $2, geometry = ST_GeomFromGeoJSON($3),
			    is_active = $4, space_id = $5, updated_at = NOW()
			WHERE geofence_id = $6
			RETURNING created_at, updated_at
		`

		err = tx.QueryRowContext(txCtx, updateQuery,
			nameVal, typeZoneVal, geometryVal, isActiveVal, spaceIDVal, geofenceID).Scan(
			&g.CreatedAt, &g.UpdatedAt,
		)
		if err != nil {
			return fmt.Errorf("failed to update geofence: %w", err)
		}

		g.GeofenceID = geofenceID
		g.Name = nameVal
		g.TypeZone = typeZoneVal
		g.Geometry = models.Geometry(geometryVal)
		g.IsActive = isActiveVal

		if spaceIDVal.Valid {
			parsedID, _ := uuid.Parse(spaceIDVal.String)
			g.SpaceID = &parsedID
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
		result, err := tx.ExecContext(txCtx, `DELETE FROM geofences WHERE geofence_id = $1`, geofenceID)
		if err != nil {
			return fmt.Errorf("failed to delete geofence: %w", err)
		}

		rows, _ := result.RowsAffected()
		if rows == 0 {
			return fmt.Errorf("geofence not found")
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