package timescaledb

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"hash/crc32"

	"github.com/Space-DF/telemetry-service/internal/models"
	"github.com/stephenafamo/bob"
)

// EventType constants
const (
	EventTypeStateChanged = "state_changed"
	EventTypeAutomation   = "automation_triggered"
)

// Pagination constants
const (
	DefaultPage       = 1
	DefaultPageSize   = 20
	MaxPageSize       = 100
	DefaultEventLimit = 100
)

// GetEventsByDevice retrieves all events for a specific entity.
func (c *Client) GetEventsByDevice(ctx context.Context, org, deviceID string, limit int, startTime, endTime *int64) ([]models.Event, error) {
	if org == "" {
		return nil, fmt.Errorf("organization is required")
	}
	if deviceID == "" {
		return nil, fmt.Errorf("device_id is required")
	}
	if limit <= 0 {
		limit = DefaultEventLimit
	}

	var events []models.Event

	err := c.WithOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
		// Build base query with device_id filter
		whereClause := `ed.shared_data->>'device_id' = $1`
		args := []interface{}{deviceID}

		// Add time range filters if provided
		argIndex := 2
		if startTime != nil {
			whereClause += fmt.Sprintf(" AND e.time_fired_ts >= $%d", argIndex)
			args = append(args, *startTime)
			argIndex++
		}
		if endTime != nil {
			whereClause += fmt.Sprintf(" AND e.time_fired_ts <= $%d", argIndex)
			args = append(args, *endTime)
			argIndex++
		}
		args = append(args, limit)

		// Complete the query
		query := fmt.Sprintf(`
			SELECT e.event_id, e.event_type_id, e.data_id, sp.space_slug,
				   e.trigger_id, e.time_fired_ts, et.event_type, ed.shared_data
			FROM events e
			JOIN event_types et ON e.event_type_id = et.event_type_id
			LEFT JOIN event_data ed ON e.data_id = ed.data_id
			LEFT JOIN spaces sp ON e.space_id = sp.space_id
			WHERE %s
			ORDER BY e.time_fired_ts DESC
			LIMIT $%d
		`, whereClause, argIndex)

		rows, err := tx.QueryContext(txCtx, query, args...)
		if err != nil {
			return fmt.Errorf("failed to query events by device: %w", err)
		}
		defer func() {
			_ = rows.Close()
		}()

		for rows.Next() {
			var e models.Event
			var dataID sql.NullInt64
			var slug sql.NullString
			var contextID []byte
			var triggerID sql.NullString
			var sharedData []byte

			if err := rows.Scan(&e.EventID, &e.EventTypeID, &dataID, &slug, &contextID, &triggerID, &e.TimeFiredTs, &e.EventType, &sharedData); err != nil {
				return err
			}

			if dataID.Valid {
				e.DataID = &dataID.Int64
			}
			if slug.Valid {
				e.SpaceSlug = slug.String
			}
			if triggerID.Valid {
				e.TriggerID = &triggerID.String
			}
			if len(sharedData) > 0 {
				e.SharedData = sharedData
			}

			events = append(events, e)
		}

		return rows.Err()
	})

	if err != nil {
		return nil, err
	}

	return events, nil
}

// populateEventRuleResponse populates an EventRuleResponse from request data
func populateEventRuleResponse(result *models.EventRuleResponse, req *models.EventRuleRequest) {
	if req.DeviceID != nil {
		result.DeviceID = req.DeviceID
	}
	if req.SpaceID != nil {
		result.SpaceID = req.SpaceID
	}
	if req.GeofenceID != nil {
		result.GeofenceID = req.GeofenceID
	}
	if req.RuleKey != nil {
		result.RuleKey = req.RuleKey
	}
	if req.Definition != nil {
		result.Definition = req.Definition
	}
	if req.IsActive != nil {
		result.IsActive = req.IsActive
	}
	if req.RepeatAble != nil {
		result.RepeatAble = req.RepeatAble
	}
	if req.CooldownSec != nil {
		result.CooldownSec = req.CooldownSec
	}
	if req.Description != nil {
		result.Description = req.Description
	}
}

// GetEventRules retrieves event rules with pagination
func (c *Client) GetEventRules(ctx context.Context, deviceID string, page, pageSize int) ([]models.EventRule, int, error) {
	if page <= 0 {
		page = DefaultPage
	}
	if pageSize <= 0 || pageSize > MaxPageSize {
		pageSize = DefaultPageSize
	}

	offset := (page - 1) * pageSize

	var rules []models.EventRule
	var total int

	org := orgFromContext(ctx)
	if org == "" {
		return nil, 0, fmt.Errorf("organization not found in context")
	}

	err := c.WithOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
		// Count total
		countQuery := `SELECT COUNT(*) FROM event_rules`
		args := []interface{}{}

		whereClause := ""
		if deviceID != "" {
			whereClause = " WHERE device_id = $1"
			args = append(args, deviceID)
		}

		countQuery += whereClause
		err := tx.QueryRowContext(txCtx, countQuery, args...).Scan(&total)
		if err != nil {
			return fmt.Errorf("failed to count event rules: %w", err)
		}

		// Query rules
		query := `
			SELECT er.event_rule_id, er.device_id, er.space_id, er.geofence_id, er.rule_key,
				   er.definition, er.is_active, er.repeat_able, er.cooldown_sec, er.description,
				   er.created_at, er.updated_at
			FROM event_rules er
		` + whereClause + ` ORDER BY er.created_at DESC LIMIT $` + fmt.Sprintf("%d", len(args)+1) + ` OFFSET $` + fmt.Sprintf("%d", len(args)+2)
		args = append(args, pageSize, offset)

		rows, err := tx.QueryContext(txCtx, query, args...)
		if err != nil {
			return fmt.Errorf("failed to query event rules: %w", err)
		}
		defer func() { _ = rows.Close() }()

		for rows.Next() {
			var r models.EventRule
			if err := rows.Scan(
				&r.EventRuleID, &r.DeviceID, &r.SpaceID, &r.GeofenceID, &r.RuleKey,
				&r.Definition, &r.IsActive, &r.RepeatAble, &r.CooldownSec, &r.Description,
				&r.CreatedAt, &r.UpdatedAt,
			); err != nil {
				return err
			}

			rules = append(rules, r)
		}

		return rows.Err()
	})

	if err != nil {
		return nil, 0, err
	}

	return rules, total, nil
}

// GetActiveRulesForDevice retrieves active automation rules for a specific device
// Returns only device-specific automation rules created by users
// If no automation rules exist, the caller should fall back to default system rules
func (c *Client) GetActiveRulesForDevice(ctx context.Context, deviceID string) ([]models.EventRule, error) {
	var rules []models.EventRule

	org := orgFromContext(ctx)
	if org == "" {
		return nil, fmt.Errorf("organization not found in context")
	}

	err := c.WithOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
		// Query automation rules for this specific device only
		query := `
			SELECT er.event_rule_id, er.device_id, er.space_id, er.geofence_id, er.rule_key,
				   er.definition, er.is_active, er.repeat_able, er.cooldown_sec, er.description,
				   er.created_at, er.updated_at
			FROM event_rules er
			WHERE er.is_active = true
			  AND (
			    er.device_id = $1
			    OR (
			      er.geofence_id IS NOT NULL
			      AND er.device_id IS NULL
			      AND er.space_id = (
			        SELECT e.space_id FROM entities e WHERE e.device_id = $1 LIMIT 1
			      )
			    )
			  )
			ORDER BY er.created_at DESC
		`

		rows, err := tx.QueryContext(txCtx, query, deviceID)
		if err != nil {
			return fmt.Errorf("failed to query event rules: %w", err)
		}
		defer func() { _ = rows.Close() }()

		for rows.Next() {
			var r models.EventRule
			if err := rows.Scan(
				&r.EventRuleID, &r.DeviceID, &r.SpaceID, &r.GeofenceID, &r.RuleKey,
				&r.Definition, &r.IsActive, &r.RepeatAble, &r.CooldownSec, &r.Description,
				&r.CreatedAt, &r.UpdatedAt,
			); err != nil {
				return err
			}

			rules = append(rules, r)
		}

		return rows.Err()
	})

	if err != nil {
		return nil, err
	}

	return rules, nil
}

// GetEventRulesByGeofenceID retrieves event rules for a specific geofence
func (c *Client) GetEventRulesByGeofenceID(ctx context.Context, geofenceID string) ([]models.EventRule, error) {
	if geofenceID == "" {
		return nil, fmt.Errorf("geofence_id is required")
	}

	org := orgFromContext(ctx)
	if org == "" {
		return nil, fmt.Errorf("organization not found in context")
	}

	var rules []models.EventRule

	err := c.WithOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
		query := `
			SELECT er.event_rule_id, er.device_id, er.space_id, er.geofence_id, er.rule_key,
			       er.definition, er.is_active, er.repeat_able, er.cooldown_sec, er.description,
			       er.created_at, er.updated_at
			FROM event_rules er
			WHERE er.geofence_id = $1
			ORDER BY er.created_at DESC
		`

		rows, err := tx.QueryContext(txCtx, query, geofenceID)
		if err != nil {
			return fmt.Errorf("failed to query event rules: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var r models.EventRule
			if err := rows.Scan(
				&r.EventRuleID, &r.DeviceID, &r.SpaceID, &r.GeofenceID, &r.RuleKey,
				&r.Definition, &r.IsActive, &r.RepeatAble, &r.CooldownSec, &r.Description,
				&r.CreatedAt, &r.UpdatedAt,
			); err != nil {
				return fmt.Errorf("failed to scan event rule: %w", err)
			}
			rules = append(rules, r)
		}

		return rows.Err()
	})

	if err != nil {
		return nil, err
	}

	return rules, nil
}

// CreateEventRule creates a new event rule
func (c *Client) CreateEventRule(ctx context.Context, req *models.EventRuleRequest) (*models.EventRuleResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("nil request")
	}

	var result models.EventRuleResponse

	org := orgFromContext(ctx)
	if org == "" {
		return nil, fmt.Errorf("organization not found in context")
	}

	err := c.WithOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
		// Insert event rule
		err := tx.QueryRowContext(txCtx, `
			INSERT INTO event_rules (device_id, space_id, geofence_id, rule_key, definition, is_active, repeat_able, cooldown_sec, description)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
			RETURNING event_rule_id, created_at, updated_at
		`, req.DeviceID, req.SpaceID, req.GeofenceID, req.RuleKey, req.Definition,
			req.IsActive, req.RepeatAble, req.CooldownSec, req.Description).Scan(
			&result.EventRuleID, &result.CreatedAt, &result.UpdatedAt,
		)

		if err != nil {
			return fmt.Errorf("failed to insert event rule: %w", err)
		}
		populateEventRuleResponse(&result, req)

		return nil
	})

	if err != nil {
		return nil, err
	}

	return &result, nil
}

// UpdateEventRule updates an existing event rule
func (c *Client) UpdateEventRule(ctx context.Context, ruleID string, req *models.EventRuleRequest) (*models.EventRuleResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("nil request")
	}
	if ruleID == "" {
		return nil, fmt.Errorf("rule_id is required")
	}

	var result models.EventRuleResponse

	org := orgFromContext(ctx)
	if org == "" {
		return nil, fmt.Errorf("organization not found in context")
	}

	err := c.WithOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
		// Update event rule
		err := tx.QueryRowContext(txCtx, `
			UPDATE event_rules
			SET device_id = $1, space_id = $2, geofence_id = $3, rule_key = $4, definition = $5,
			    is_active = $6, repeat_able = $7, cooldown_sec = $8, description = $9, updated_at = NOW()
			WHERE event_rule_id = $10
			RETURNING event_rule_id, created_at, updated_at
		`, req.DeviceID, req.SpaceID, req.GeofenceID, req.RuleKey, req.Definition,
			req.IsActive, req.RepeatAble, req.CooldownSec, req.Description, ruleID).Scan(
			&result.EventRuleID, &result.CreatedAt, &result.UpdatedAt,
		)

		if err != nil {
			return fmt.Errorf("failed to update event rule: %w", err)
		}
		populateEventRuleResponse(&result, req)

		return nil
	})

	if err != nil {
		return nil, err
	}

	return &result, nil
}

// DeleteEventRule deletes an event rule
func (c *Client) DeleteEventRule(ctx context.Context, ruleID string) error {
	if ruleID == "" {
		return fmt.Errorf("rule_id is required")
	}

	org := orgFromContext(ctx)
	if org == "" {
		return fmt.Errorf("organization not found in context")
	}

	return c.WithOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
		result, err := tx.ExecContext(txCtx, `DELETE FROM event_rules WHERE event_rule_id = $1`, ruleID)
		if err != nil {
			return fmt.Errorf("failed to delete event rule: %w", err)
		}

		rows, _ := result.RowsAffected()
		if rows == 0 {
			return fmt.Errorf("event rule not found")
		}

		return nil
	})
}

// CreateEvent creates a new event from a matched event rule
func (c *Client) CreateEvent(ctx context.Context, org string, event *models.MatchedEvent, spaceSlug string) error {
	if event == nil {
		return fmt.Errorf("nil event")
	}
	if org == "" {
		return fmt.Errorf("organization is required")
	}

	return c.WithOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
		// Get event_type
		var eventTypeID int
		err := tx.QueryRowContext(txCtx, `
			SELECT event_type_id FROM event_types WHERE event_type = $1
		`, event.EventType).Scan(&eventTypeID)

		if err != nil {
			if err == sql.ErrNoRows {
				return fmt.Errorf("event type '%s' does not exist", event.EventType)
			}
			return fmt.Errorf("failed to get event_type: %w", err)
		}

		// Create event_data with event details
		dataID := sql.NullInt64{Valid: false}
		eventData := map[string]interface{}{
			"description": event.Description,
			"value":       event.Value,
			"threshold":   event.Threshold,
			"operator":    event.Operator,
			"rule_key":    event.RuleKey,
		}

		rawData, err := json.Marshal(eventData)
		if err != nil {
			return fmt.Errorf("failed to marshal event data: %w", err)
		}

		hash := int64(crc32.ChecksumIEEE(rawData))
		err = tx.QueryRowContext(txCtx, `
			INSERT INTO event_data (hash, shared_data)
			VALUES ($1, $2)
			ON CONFLICT (hash) DO UPDATE SET shared_data = EXCLUDED.shared_data
			RETURNING data_id
		`, hash, rawData).Scan(&dataID)

		if err != nil {
			return fmt.Errorf("failed to create event_data: %w", err)
		}

		// Create the event
		_, err = tx.ExecContext(txCtx, `
			INSERT INTO events (
				event_type_id, data_id, event_level, event_rule_id,
				space_id, entity_id, state_id, time_fired_ts
			) VALUES ($1, $2, $3, $4, (SELECT space_id FROM spaces WHERE space_slug = $5 LIMIT 1), $6, $7, $8)
		`, eventTypeID, dataID, event.EventLevel, event.EventRuleID, spaceSlug, event.EntityID, event.StateID, event.Timestamp)

		if err != nil {
			return fmt.Errorf("failed to create event: %w", err)
		}

		return nil
	})
}
