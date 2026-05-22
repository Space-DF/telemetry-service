package timescaledb

import (
	"context"
	"fmt"

	"github.com/Space-DF/telemetry-service/internal/models"
	"github.com/google/uuid"
	"github.com/stephenafamo/bob"
	"go.uber.org/zap"
)

// UpsertSpace inserts or updates a space in the organization schema.
// This is called by the Celery task consumer when update_space task is received.
func (c *Client) UpsertSpace(ctx context.Context, orgSlug string, space models.SpaceData) error {
	if orgSlug == "" {
		return fmt.Errorf("organization slug is required")
	}

	err := c.WithOrgTx(ctx, orgSlug, func(txCtx context.Context, tx bob.Tx) error {
		var hasIsDefault bool
		err := tx.QueryRowContext(txCtx, `
			SELECT EXISTS (
				SELECT 1
				FROM information_schema.columns
				WHERE table_schema = current_schema()
					AND table_name = 'spaces'
					AND column_name = 'is_default'
			)
		`).Scan(&hasIsDefault)
		if err != nil {
			return fmt.Errorf("failed to check spaces.is_default column in org '%s': %w", orgSlug, err)
		}

		var query string
		var args []interface{}

		if hasIsDefault {
			query = `
				INSERT INTO spaces (space_id, name, logo, space_slug, is_active, is_default, total_devices, description, created_by)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
				ON CONFLICT (space_id) DO UPDATE
					SET name = EXCLUDED.name,
						logo = EXCLUDED.logo,
						space_slug = EXCLUDED.space_slug,
						is_active = EXCLUDED.is_active,
						is_default = EXCLUDED.is_default,
						total_devices = EXCLUDED.total_devices,
						description = EXCLUDED.description,
						created_by = EXCLUDED.created_by
			`
			args = []interface{}{
				space.ID,
				space.Name,
				space.Logo,
				space.SlugName,
				space.IsActive,
				space.IsDefault,
				space.TotalDevices,
				space.Description,
				space.CreatedBy,
			}
		} else {
			query = `
				INSERT INTO spaces (space_id, name, logo, space_slug, is_active, total_devices, description, created_by)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
				ON CONFLICT (space_id) DO UPDATE
					SET name = EXCLUDED.name,
						logo = EXCLUDED.logo,
						space_slug = EXCLUDED.space_slug,
						is_active = EXCLUDED.is_active,
						total_devices = EXCLUDED.total_devices,
						description = EXCLUDED.description,
						created_by = EXCLUDED.created_by
			`
			args = []interface{}{
				space.ID,
				space.Name,
				space.Logo,
				space.SlugName,
				space.IsActive,
				space.TotalDevices,
				space.Description,
				space.CreatedBy,
			}
		}

		_, err = tx.ExecContext(txCtx, query, args...)

		if err != nil {
			return fmt.Errorf("failed to upsert space '%s' in org '%s': %w", space.SlugName, orgSlug, err)
		}

		return nil
	})

	if err != nil {
		return err
	}

	c.Logger.Info("Space upserted successfully",
		zap.String("org", orgSlug),
		zap.String("space_slug", space.SlugName),
		zap.String("space_id", space.ID.String()))

	return nil
}

// GetSpaceIDBySlug looks up a space by its slug within the organization schema
func (c *Client) GetSpaceIDBySlug(ctx context.Context, orgSlug, spaceSlug string) (uuid.UUID, error) {
	if orgSlug == "" || spaceSlug == "" {
		return uuid.Nil, fmt.Errorf("organization slug and space slug are required")
	}

	var spaceID uuid.UUID
	err := c.WithOrgTx(ctx, orgSlug, func(txCtx context.Context, tx bob.Tx) error {
		return tx.QueryRowContext(txCtx,
			`SELECT space_id FROM spaces WHERE space_slug = $1 LIMIT 1`, spaceSlug,
		).Scan(&spaceID)
	})
	if err != nil {
		return uuid.Nil, fmt.Errorf("space with slug '%s' not found in org '%s': %w", spaceSlug, orgSlug, err)
	}

	return spaceID, nil
}

// DeleteSpace deletes a space from the organization schema.
// This is called by the Celery task consumer when delete_space task is received.
func (c *Client) DeleteSpace(ctx context.Context, orgSlug string, spaceID uuid.UUID) error {
	if orgSlug == "" {
		return fmt.Errorf("organization slug is required")
	}
	if spaceID == uuid.Nil {
		return fmt.Errorf("space_id is required")
	}

	err := c.WithOrgTx(ctx, orgSlug, func(txCtx context.Context, tx bob.Tx) error {
		// Delete entity_states (telemetry data) for entities in this space
		_, err := tx.ExecContext(txCtx, `
			DELETE FROM entity_states 
			WHERE entity_id IN (
				SELECT e.id FROM entities e 
				WHERE e.space_id = $1
			)
		`, spaceID)
		if err != nil {
			return fmt.Errorf("failed to delete entity_states for space '%s' in org '%s': %w", spaceID, orgSlug, err)
		}

		// Delete orphaned entity_state_attributes
		_, err = tx.ExecContext(txCtx, `
			DELETE FROM entity_state_attributes 
			WHERE id NOT IN (
				SELECT DISTINCT attributes_id FROM entity_states WHERE attributes_id IS NOT NULL
			)
		`)
		if err != nil {
			return fmt.Errorf("failed to delete entity_state_attributes for space '%s' in org '%s': %w", spaceID, orgSlug, err)
		}

		// Delete entities (devices/sensors) in this space
		_, err = tx.ExecContext(txCtx, `
			DELETE FROM entities 
			WHERE space_id = $1
		`, spaceID)
		if err != nil {
			return fmt.Errorf("failed to delete entities for space '%s' in org '%s': %w", spaceID, orgSlug, err)
		}

		//  Delete geofences in this space (if any)
		_, err = tx.ExecContext(txCtx, `
			DELETE FROM geofences 
			WHERE space_id = $1
		`, spaceID)
		if err != nil {
			c.Logger.Warn("Failed to delete geofences (table may not exist)",
				zap.String("org", orgSlug),
				zap.String("space_id", spaceID.String()),
				zap.Error(err))
		}

		// Delete event rules associated with automations in this space
		_, err = tx.ExecContext(txCtx, `
			DELETE FROM event_rules
			WHERE event_rule_id IN (
				SELECT event_rule_id FROM automations WHERE space_id = $1
			)
		`, spaceID)
		if err != nil {
			c.Logger.Warn("Failed to delete event_rules for space",
				zap.String("org", orgSlug),
				zap.String("space_id", spaceID.String()),
				zap.Error(err))
		}

		// Delete automations associated with this space
		_, err = tx.ExecContext(txCtx, `DELETE FROM automations WHERE space_id = $1`, spaceID)
		if err != nil {
			c.Logger.Warn("Failed to delete automations for space",
				zap.String("org", orgSlug),
				zap.String("space_id", spaceID.String()),
				zap.Error(err))
		}

		// Delete events associated with devices in this space
		_, err = tx.ExecContext(txCtx, `
			DELETE FROM events
			WHERE device_id IN (
				SELECT device_id FROM entities WHERE space_id = $1
			)
		`, spaceID)
		if err != nil {
			c.Logger.Warn("Failed to delete events for space",
				zap.String("org", orgSlug),
				zap.String("space_id", spaceID.String()),
				zap.Error(err))
		}

		//  Delete the space itself
		_, err = tx.ExecContext(txCtx, `DELETE FROM spaces WHERE space_id = $1`, spaceID)
		if err != nil {
			return fmt.Errorf("failed to delete space '%s' in org '%s': %w", spaceID, orgSlug, err)
		}

		return nil
	})

	if err != nil {
		return err
	}

	c.Logger.Info("Space and all related telemetry data deleted successfully",
		zap.String("org", orgSlug),
		zap.String("space_id", spaceID.String()))

	return nil
}

// DeleteDeviceFromSpace deletes all telemetry data for a specific device in a space.
func (c *Client) DeleteDeviceFromSpace(ctx context.Context, orgSlug string, deviceID uuid.UUID) error {
	if orgSlug == "" {
		return fmt.Errorf("organization slug is required")
	}
	if deviceID == uuid.Nil {
		return fmt.Errorf("device_id is required")
	}

	err := c.WithOrgTx(ctx, orgSlug, func(txCtx context.Context, tx bob.Tx) error {
		// Delete entity_states (telemetry data) for this device
		_, err := tx.ExecContext(txCtx, `
			DELETE FROM entity_states 
			WHERE entity_id IN (
				SELECT e.id FROM entities e 
				WHERE e.device_id = $1
			)
		`, deviceID)
		if err != nil {
			return fmt.Errorf("failed to delete entity_states for device '%s': %w", deviceID, err)
		}

		// Delete orphaned entity_state_attributes
		_, err = tx.ExecContext(txCtx, `
			DELETE FROM entity_state_attributes 
			WHERE id NOT IN (
				SELECT DISTINCT attributes_id FROM entity_states WHERE attributes_id IS NOT NULL
			)
		`)
		if err != nil {
			return fmt.Errorf("failed to delete entity_state_attributes for device '%s': %w", deviceID, err)
		}

		// Delete entities for this device
		_, err = tx.ExecContext(txCtx, `DELETE FROM entities WHERE device_id = $1`, deviceID)
		if err != nil {
			return fmt.Errorf("failed to delete entities for device '%s': %w", deviceID, err)
		}

		// Delete event rules associated with this device's automations
		_, err = tx.ExecContext(txCtx, `
			DELETE FROM event_rules
			WHERE event_rule_id IN (
				SELECT event_rule_id FROM automations WHERE device_id = $1
			)
		`, deviceID)
		if err != nil {
			return fmt.Errorf("failed to delete event_rules for device '%s': %w", deviceID, err)
		}

		// Delete automations associated with this device
		_, err = tx.ExecContext(txCtx, `DELETE FROM automations WHERE device_id = $1`, deviceID)
		if err != nil {
			return fmt.Errorf("failed to delete automations for device '%s': %w", deviceID, err)
		}

		// Delete events associated with this device
		_, err = tx.ExecContext(txCtx, `DELETE FROM events WHERE device_id = $1`, deviceID)
		if err != nil {
			return fmt.Errorf("failed to delete events for device '%s': %w", deviceID, err)
		}

		return nil
	})

	if err != nil {
		return err
	}

	return nil
}
