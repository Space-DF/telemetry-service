package timescaledb

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"log"
	"time"

	"github.com/Space-DF/telemetry-service/internal/models"
	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/stephenafamo/bob"
)

func (c *Client) SaveTelemetryPayload(ctx context.Context, payload *models.TelemetryPayload) error {
	if payload == nil {
		return fmt.Errorf("nil telemetry payload")
	}

	org := payload.Organization
	if org == "" {
		return fmt.Errorf("missing organization in telemetry payload")
	}

	return c.WithOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
		for i := range payload.Entities {
			stateID, err := c.upsertTelemetryEntity(txCtx, tx, &payload.Entities[i], payload)
			if err != nil {
				log.Printf("[Telemetry] ERROR upserting entity: %v", err)
				return err
			}
			payload.Entities[i].StateID = stateID
		}
		return nil
	})
}

func (c *Client) upsertTelemetryEntity(ctx context.Context, tx bob.Tx, ent *models.TelemetryEntity, payload *models.TelemetryPayload) (uuid.UUID, error) {
	if ent == nil {
		return uuid.UUID{}, fmt.Errorf("nil telemetry entity")
	}

	displayType := ent.DisplayType
	if len(displayType) == 0 {
		displayType = []string{"unknown"}
	}

	// Ensure entity type exists (unique by unique_key).
	entityTypeKey := ent.EntityType
	if entityTypeKey == "" {
		entityTypeKey = "unknown"
	}

	var entityTypeID uuid.UUID
	if err := tx.QueryRowContext(ctx, `
		INSERT INTO entity_types (id, name, unique_key, created_at, updated_at)
		VALUES ($1, $2, $3, now(), now())
		ON CONFLICT (unique_key) DO UPDATE SET name = EXCLUDED.name, updated_at = now()
		RETURNING id`,
		uuid.New(),
		ent.EntityType,
		entityTypeKey,
	).Scan(&entityTypeID); err != nil {
		return uuid.UUID{}, fmt.Errorf("upsert entity_type '%s': %w", entityTypeKey, err)
	}

	// Prepare optional device_id.
	var deviceUUID *uuid.UUID
	if payload.DeviceID != "" {
		if parsed, err := uuid.Parse(payload.DeviceID); err == nil {
			deviceUUID = &parsed
		}
	}

	// Upsert entity row.
	var entityID uuid.UUID
	if err := tx.QueryRowContext(ctx, `
		INSERT INTO entities (
			id, space_id, device_id, unique_key, category, entity_type_id,
			name, unit_of_measurement, display_type, is_enabled, created_at, updated_at
		)
		VALUES ($1, (SELECT space_id FROM spaces WHERE space_slug = $2 LIMIT 1), $3, $4, $5, $6, $7, $8, $9, true, now(), now())
		ON CONFLICT (unique_key) DO UPDATE SET
			space_id = EXCLUDED.space_id,
			device_id = EXCLUDED.device_id,
			name = EXCLUDED.name,
			unit_of_measurement = EXCLUDED.unit_of_measurement,
			category = EXCLUDED.category,
			entity_type_id = EXCLUDED.entity_type_id,
			display_type = EXCLUDED.display_type,
			updated_at = now()
		RETURNING id`,
		uuid.New(),
		payload.SpaceSlug,
		deviceUUID,
		ent.UniqueID,
		ent.EntityType,
		entityTypeID,
		ent.Name,
		ent.UnitOfMeas,
		pq.Array(displayType),
	).Scan(&entityID); err != nil {
		return uuid.UUID{}, fmt.Errorf("upsert entity '%s': %w", ent.UniqueID, err)
	}

	// Handle attributes: deduplicate by hash to reuse existing row.
	var attrsID sql.NullString
	if len(ent.Attributes) > 0 {
		rawAttrs, err := json.Marshal(ent.Attributes)
		if err != nil {
			return uuid.UUID{}, fmt.Errorf("marshal attributes for '%s': %w", ent.UniqueID, err)
		}

		hash := int64(crc32.ChecksumIEEE(rawAttrs))
		if err := tx.QueryRowContext(ctx, `
			INSERT INTO entity_state_attributes (id, hash, shared_attrs)
			VALUES ($1, $2, $3)
			ON CONFLICT (hash) DO UPDATE SET shared_attrs = EXCLUDED.shared_attrs
			RETURNING id`,
			uuid.New(),
			hash,
			rawAttrs,
		).Scan(&attrsID); err != nil {
			return uuid.UUID{}, fmt.Errorf("upsert attributes for '%s': %w", ent.UniqueID, err)
		}
	}

	// Parse timestamps.
	reportedAt := parseRFC3339(ent.Timestamp)
	if reportedAt.IsZero() {
		reportedAt = parseRFC3339(payload.Timestamp)
	}
	if reportedAt.IsZero() {
		reportedAt = time.Now().UTC()
	}

	stateStr := fmt.Sprint(ent.State)

	// Get the most recent state for this entity to check if it changed
	var lastStateID sql.NullString
	var lastState sql.NullString
	var lastChangedAt time.Time
	var lastReportedAt time.Time
	err := tx.QueryRowContext(ctx, `
		SELECT id, state, last_changed_at, reported_at
		FROM entity_states
		WHERE entity_id = $1 AND reported_at < $2
		ORDER BY reported_at DESC
		LIMIT 1`,
		entityID,
		reportedAt,
	).Scan(&lastStateID, &lastState, &lastChangedAt, &lastReportedAt)

	// Determine last_changed_at: if state value changed, use reportedAt; otherwise keep old timestamp
	changedAt := reportedAt
	if err == nil && lastState.Valid && lastState.String == stateStr {
		// State hasn't changed, preserve the last_changed_at timestamp
		changedAt = lastChangedAt
	}

	// Prepare old_state_id for linking
	var oldStateUUID *uuid.UUID
	if lastStateID.Valid {
		if parsed, err := uuid.Parse(lastStateID.String); err == nil {
			oldStateUUID = &parsed
		}
	}

	stateID := uuid.New()
	_, err = tx.ExecContext(ctx, `
		INSERT INTO entity_states (
			id, entity_id, state, attributes_id, old_state_id, reported_at, last_changed_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		stateID,
		entityID,
		stateStr,
		nullUUID(attrsID),
		oldStateUUID,
		reportedAt,
		changedAt,
	)
	if err != nil {
		return uuid.UUID{}, fmt.Errorf("insert entity_state for '%s': %w", ent.UniqueID, err)
	}

	return stateID, nil
}

func parseRFC3339(ts string) time.Time {
	if ts == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339, ts)
	if err != nil {
		return time.Time{}
	}
	return t.UTC()
}

func nullUUID(id sql.NullString) any {
	if id.Valid && id.String != "" {
		return id.String
	}
	return nil
}
