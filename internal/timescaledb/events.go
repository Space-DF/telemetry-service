package timescaledb

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/Space-DF/telemetry-service/internal/api/common"
	"github.com/Space-DF/telemetry-service/internal/models"
	"github.com/stephenafamo/bob"
)

// GetEventsByDevice retrieves events for a specific device with pagination.
func (c *Client) GetEventsByDevice(ctx context.Context, org, deviceID string, limit, offset int, startTime, endTime *int64, titleSearch *string) ([]models.Event, int, error) {
	if org == "" {
		return nil, 0, fmt.Errorf("organization is required")
	}
	if deviceID == "" {
		return nil, 0, fmt.Errorf("device_id is required")
	}
	if limit <= 0 {
		limit = common.DefaultLimit
	}

	var events []models.Event
	var total int

	err := c.WithOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
		// Build base query with device_id filter — uses the indexed e.device_id column
		whereClause := `e.device_id = $1::uuid`
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
		// Add title search filter if provided
		if titleSearch != nil && *titleSearch != "" {
			whereClause += fmt.Sprintf(" AND e.title ILIKE $%d", argIndex)
			args = append(args, "%"+*titleSearch+"%")
			argIndex++
		}

		// Count total matching events
		countQuery := fmt.Sprintf(`SELECT COUNT(*) FROM events e WHERE %s`, whereClause)
		if err := tx.QueryRowContext(txCtx, countQuery, args...).Scan(&total); err != nil {
			return fmt.Errorf("failed to count events: %w", err)
		}

		args = append(args, limit, offset)

		// Complete the query — JOIN automations and geofences for detail info
		query := fmt.Sprintf(`
			SELECT e.event_id, e.event_type_id, sp.space_slug,
				   e.device_id, e.title, e.time_fired_ts, et.event_type,
				   e.event_level, e.event_rule_id,
				   e.automation_id, a.name AS automation_name, a.device_id AS automation_device_id,
				   e.geofence_id, g.name AS geofence_name, g.type_zone AS geofence_type_zone,
				   e.location
			FROM events e
			JOIN event_types et ON e.event_type_id = et.event_type_id
			LEFT JOIN spaces sp ON e.space_id = sp.space_id
			LEFT JOIN automations a ON e.automation_id = a.id
			LEFT JOIN geofences g ON e.geofence_id = g.geofence_id
			WHERE %s
			ORDER BY e.time_fired_ts DESC
			LIMIT $%d OFFSET $%d
		`, whereClause, argIndex, argIndex+1)

		rows, err := tx.QueryContext(txCtx, query, args...)
		if err != nil {
			return fmt.Errorf("failed to query events by device: %w", err)
		}
		defer func() {
			_ = rows.Close()
		}()

		for rows.Next() {
			var e models.Event
			var slug sql.NullString
			var deviceIDVal sql.NullString
			var titleVal sql.NullString
			var eventLevel, eventRuleID sql.NullString
			var automationID, automationName, automationDeviceID sql.NullString
			var geofenceID, geofenceName, geofenceTypeZone sql.NullString
			var locationJSON []byte

			if err := rows.Scan(
				&e.EventID, &e.EventTypeID, &slug,
				&deviceIDVal, &titleVal, &e.TimeFiredTs, &e.EventType,
				&eventLevel, &eventRuleID,
				&automationID, &automationName, &automationDeviceID,
				&geofenceID, &geofenceName, &geofenceTypeZone,
				&locationJSON,
			); err != nil {
				return err
			}

			if slug.Valid {
				e.SpaceSlug = slug.String
			}
			if deviceIDVal.Valid {
				e.DeviceID = deviceIDVal.String
			}
			if titleVal.Valid {
				e.Title = titleVal.String
			}
			if eventLevel.Valid {
				e.EventLevel = &eventLevel.String
			}
			if eventRuleID.Valid {
				e.EventRuleID = &eventRuleID.String
			}
			if automationID.Valid {
				e.AutomationID = &automationID.String
				if automationName.Valid {
					e.AutomationName = &automationName.String
				}
				if automationDeviceID.Valid {
					e.AutomationDeviceID = automationDeviceID.String
				}
			}
			if geofenceID.Valid {
				e.GeofenceID = &geofenceID.String
				if geofenceName.Valid {
					e.GeofenceName = &geofenceName.String
				}
				if geofenceTypeZone.Valid {
					e.GeofenceTypeZone = &geofenceTypeZone.String
				}
			}

			// Parse location from JSONB
			if len(locationJSON) > 0 {
				var loc models.Location
				if err := json.Unmarshal(locationJSON, &loc); err == nil {
					e.Location = &loc
				}
			}

			events = append(events, e)
		}

		return rows.Err()
	})

	if err != nil {
		return nil, 0, err
	}

	return events, total, nil
}

// CreateEvent creates a new event from a matched automation or geofence
func (c *Client) CreateEvent(ctx context.Context, org string, event *models.MatchedEvent, spaceSlug, deviceID string) error {
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

		var locationJSON []byte
		if event.Location != nil {
			locationJSON, _ = json.Marshal(event.Location)
		}

		// Create the event - use event_rule_id, automation_id, geofence_id, and device_id from the matched event
		// Return only event_id; names are already in MatchedEvent
		var eventID int64
		err = tx.QueryRowContext(txCtx, `
			INSERT INTO events (
				event_type_id, event_level, event_rule_id, automation_id, geofence_id,
				space_id, device_id, state_id, location, time_fired_ts, title
			) VALUES (
				$1, $2, $3, $4, $5,
				(SELECT space_id FROM spaces WHERE space_slug = $6 LIMIT 1),
				$7::uuid, $8, $9::jsonb, $10, $11
			)
			RETURNING event_id
		`, eventTypeID, event.EventLevel, event.EventRuleID, event.AutomationID, event.GeofenceID, spaceSlug, deviceID, event.StateID, locationJSON, event.Timestamp, event.Title).Scan(&eventID)

		if err != nil {
			return fmt.Errorf("failed to create event: %w", err)
		}

		// Publish event to AMQP for real-time processing
		// Use names from MatchedEvent (already fetched by the evaluator)
		eventLevel := event.EventLevel

		if err := c.PublishEventToDevice(ctx, &models.Event{
			EventID:        eventID,
			EventTypeID:    eventTypeID,
			EventType:      event.EventType,
			EventLevel:     &eventLevel,
			EventRuleID:    event.EventRuleID,
			AutomationID:   event.AutomationID,
			AutomationName: event.AutomationName,
			GeofenceID:     event.GeofenceID,
			GeofenceName:   event.GeofenceName,
			StateID:        event.StateID,
			DeviceID:       deviceID,
			SpaceSlug:      spaceSlug,
			TimeFiredTs:    event.Timestamp,
			Title:          event.Title,
			Location:       event.Location,
		}, org); err != nil {
			return fmt.Errorf("failed to publish event: %w", err)
		}

		return nil
	})
}

// Event Rules CRUD operations
// CreateEventRule creates a new event rule
func (c *Client) CreateEventRule(ctx context.Context, rule *models.EventRule) error {
	org := orgFromContext(ctx)
	if org == "" {
		return fmt.Errorf("organization not found in context")
	}

	return c.WithOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
		query := `
			INSERT INTO event_rules (rule_key, definition, is_active, repeat_able, cooldown_sec, description)
			VALUES ($1, $2, $3, $4, $5, $6)
			RETURNING event_rule_id, created_at
		`
		isActive := true
		repeatAble := true
		if rule.IsActive != nil {
			isActive = *rule.IsActive
		}
		if rule.RepeatAble != nil {
			repeatAble = *rule.RepeatAble
		}

		err := tx.QueryRowContext(txCtx, query,
			rule.RuleKey, rule.Definition, isActive, repeatAble,
			rule.CooldownSec, rule.Description,
		).Scan(&rule.EventRuleID, &rule.CreatedAt)

		if err != nil {
			return fmt.Errorf("failed to create event rule: %w", err)
		}

		return nil
	})
}

// GetEventRules retrieves all event rules
func (c *Client) GetEventRules(ctx context.Context) ([]models.EventRule, error) {
	var rules []models.EventRule

	org := orgFromContext(ctx)
	if org == "" {
		return nil, fmt.Errorf("organization not found in context")
	}

	err := c.WithOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
		query := `
			SELECT event_rule_id, rule_key, definition, is_active, repeat_able, cooldown_sec, description, created_at
			FROM event_rules
			ORDER BY created_at DESC
		`

		rows, err := tx.QueryContext(txCtx, query)
		if err != nil {
			return fmt.Errorf("failed to query event rules: %w", err)
		}
		defer func() {
			_ = rows.Close()
		}()

		for rows.Next() {
			var rule models.EventRule
			var description sql.NullString
			var definition sql.NullString
			var cooldownSec sql.NullInt64

			err := rows.Scan(
				&rule.EventRuleID, &rule.RuleKey, &definition, &rule.IsActive,
				&rule.RepeatAble, &cooldownSec, &description, &rule.CreatedAt,
			)
			if err != nil {
				return err
			}

			if description.Valid {
				rule.Description = &description.String
			}
			if definition.Valid {
				rule.Definition = json.RawMessage(definition.String)
			}
			if cooldownSec.Valid {
				val := int(cooldownSec.Int64)
				rule.CooldownSec = &val
			}

			rules = append(rules, rule)
		}

		return rows.Err()
	})

	if err != nil {
		return nil, err
	}

	return rules, nil
}

// GetEventRulesByIDs retrieves event rules by their IDs
func (c *Client) GetEventRulesByIDs(ctx context.Context, eventRuleIDs []string) ([]models.EventRule, error) {
	if len(eventRuleIDs) == 0 {
		return []models.EventRule{}, nil
	}

	org := orgFromContext(ctx)
	if org == "" {
		return nil, fmt.Errorf("organization not found in context")
	}

	var rules []models.EventRule

	err := c.WithOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
		// Build placeholders for IN clause
		placeholders := make([]string, len(eventRuleIDs))
		args := make([]interface{}, len(eventRuleIDs))
		for i, id := range eventRuleIDs {
			placeholders[i] = fmt.Sprintf("$%d", i+1)
			args[i] = id
		}

		query := fmt.Sprintf(`
			SELECT event_rule_id, rule_key, definition, is_active, repeat_able, cooldown_sec, description, created_at
			FROM event_rules
			WHERE event_rule_id IN (%s)
			ORDER BY created_at DESC
		`, strings.Join(placeholders, ", "))

		rows, err := tx.QueryContext(txCtx, query, args...)
		if err != nil {
			return fmt.Errorf("failed to query event rules by IDs: %w", err)
		}
		defer func() {
			_ = rows.Close()
		}()

		for rows.Next() {
			var rule models.EventRule
			var description sql.NullString
			var definition sql.NullString
			var cooldownSec sql.NullInt64

			err := rows.Scan(
				&rule.EventRuleID, &rule.RuleKey, &definition, &rule.IsActive,
				&rule.RepeatAble, &cooldownSec, &description, &rule.CreatedAt,
			)
			if err != nil {
				return err
			}

			if description.Valid {
				rule.Description = &description.String
			}
			if definition.Valid {
				rule.Definition = json.RawMessage(definition.String)
			}
			if cooldownSec.Valid {
				val := int(cooldownSec.Int64)
				rule.CooldownSec = &val
			}

			rules = append(rules, rule)
		}

		return rows.Err()
	})

	if err != nil {
		return nil, err
	}

	return rules, nil
}

// GetEventRuleByID retrieves an event rule by ID
func (c *Client) GetEventRuleByID(ctx context.Context, ruleID string) (*models.EventRule, error) {
	var rule models.EventRule

	org := orgFromContext(ctx)
	if org == "" {
		return nil, fmt.Errorf("organization not found in context")
	}

	err := c.WithOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
		query := `
			SELECT event_rule_id, rule_key, definition, is_active, repeat_able, cooldown_sec, description, created_at
			FROM event_rules
			WHERE event_rule_id = $1
		`

		var description sql.NullString
		var definition sql.NullString
		var cooldownSec sql.NullInt64

		err := tx.QueryRowContext(txCtx, query, ruleID).Scan(
			&rule.EventRuleID, &rule.RuleKey, &definition, &rule.IsActive,
			&rule.RepeatAble, &cooldownSec, &description, &rule.CreatedAt,
		)
		if err != nil {
			return err
		}

		if description.Valid {
			rule.Description = &description.String
		}
		if definition.Valid {
			rule.Definition = json.RawMessage(definition.String)
		}
		if cooldownSec.Valid {
			val := int(cooldownSec.Int64)
			rule.CooldownSec = &val
		}

		return nil
	})

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("event rule not found")
		}
		return nil, err
	}

	return &rule, nil
}

// GetEventRulesByGeofenceID retrieves event rules for a specific geofence
func (c *Client) GetEventRulesByGeofenceID(ctx context.Context, geofenceID string) ([]models.EventRule, error) {
	var rules []models.EventRule

	org := orgFromContext(ctx)
	if org == "" {
		return nil, fmt.Errorf("organization not found in context")
	}

	err := c.WithOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
		query := `
			SELECT er.event_rule_id, er.rule_key, er.definition, er.is_active, er.repeat_able, er.cooldown_sec, er.description, er.created_at
			FROM event_rules er
			JOIN geofences g ON g.event_rule_id = er.event_rule_id
			WHERE g.geofence_id = $1
			ORDER BY er.created_at DESC
		`

		rows, err := tx.QueryContext(txCtx, query, geofenceID)
		if err != nil {
			return fmt.Errorf("failed to query event rules for geofence: %w", err)
		}
		defer func() {
			_ = rows.Close()
		}()

		for rows.Next() {
			var rule models.EventRule
			var description sql.NullString
			var definition sql.NullString
			var cooldownSec sql.NullInt64

			err := rows.Scan(
				&rule.EventRuleID, &rule.RuleKey, &definition, &rule.IsActive,
				&rule.RepeatAble, &cooldownSec, &description, &rule.CreatedAt,
			)
			if err != nil {
				return err
			}

			if description.Valid {
				rule.Description = &description.String
			}
			if definition.Valid {
				rule.Definition = json.RawMessage(definition.String)
			}
			if cooldownSec.Valid {
				val := int(cooldownSec.Int64)
				rule.CooldownSec = &val
			}

			rules = append(rules, rule)
		}

		return rows.Err()
	})

	if err != nil {
		return nil, err
	}

	return rules, nil
}

// GetActiveRulesForDevice retrieves all active event rules for a device (via automations)
func (c *Client) GetActiveRulesForDevice(ctx context.Context, deviceID string) ([]models.EventRule, error) {
	var rules []models.EventRule

	org := orgFromContext(ctx)
	if org == "" {
		return nil, fmt.Errorf("organization not found in context")
	}

	err := c.WithOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
		query := `
			SELECT er.event_rule_id, er.rule_key, er.definition, er.is_active, er.repeat_able, er.cooldown_sec, er.description, er.created_at
			FROM event_rules er
			JOIN automations a ON a.event_rule_id = er.event_rule_id
			WHERE a.device_id = $1 AND er.is_active = true
			ORDER BY er.created_at DESC
		`

		rows, err := tx.QueryContext(txCtx, query, deviceID)
		if err != nil {
			return fmt.Errorf("failed to query active rules for device: %w", err)
		}
		defer func() {
			_ = rows.Close()
		}()

		for rows.Next() {
			var rule models.EventRule
			var description sql.NullString
			var definition sql.NullString
			var cooldownSec sql.NullInt64

			err := rows.Scan(
				&rule.EventRuleID, &rule.RuleKey, &definition, &rule.IsActive,
				&rule.RepeatAble, &cooldownSec, &description, &rule.CreatedAt,
			)
			if err != nil {
				return err
			}

			if description.Valid {
				rule.Description = &description.String
			}
			if definition.Valid {
				rule.Definition = json.RawMessage(definition.String)
			}
			if cooldownSec.Valid {
				val := int(cooldownSec.Int64)
				rule.CooldownSec = &val
			}

			rules = append(rules, rule)
		}

		return rows.Err()
	})

	if err != nil {
		return nil, err
	}

	return rules, nil
}

// UpdateEventRule updates an existing event rule
func (c *Client) UpdateEventRule(ctx context.Context, ruleID string, rule *models.EventRule) error {
	org := orgFromContext(ctx)
	if org == "" {
		return fmt.Errorf("organization not found in context")
	}

	return c.WithOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
		// Load current values from DB to use as defaults
		var currentRuleKey sql.NullString
		var currentDefinition sql.NullString
		var currentIsActive sql.NullBool
		var currentRepeatAble sql.NullBool
		var currentCooldownSec sql.NullInt64
		var currentDescription sql.NullString

		err := tx.QueryRowContext(txCtx, `
			SELECT rule_key, definition, is_active, repeat_able, cooldown_sec, description
			FROM event_rules WHERE event_rule_id = $1
		`, ruleID).Scan(
			&currentRuleKey, &currentDefinition, &currentIsActive,
			&currentRepeatAble, &currentCooldownSec, &currentDescription,
		)
		if err != nil {
			if err == sql.ErrNoRows {
				return fmt.Errorf("event rule not found")
			}
			return fmt.Errorf("failed to get current event rule: %w", err)
		}

		// Initialize defaults from current DB data
		ruleKey := currentRuleKey.String
		definition := json.RawMessage(currentDefinition.String)
		isActive := currentIsActive.Bool
		repeatAble := currentRepeatAble.Bool
		cooldownSec := int(currentCooldownSec.Int64)
		description := currentDescription.String

		// Override with provided values if set
		if rule.RuleKey != "" {
			ruleKey = rule.RuleKey
		}
		if len(rule.Definition) > 0 {
			definition = rule.Definition
		}
		if rule.IsActive != nil {
			isActive = *rule.IsActive
		}
		if rule.RepeatAble != nil {
			repeatAble = *rule.RepeatAble
		}
		if rule.CooldownSec != nil {
			cooldownSec = *rule.CooldownSec
		}
		if rule.Description != nil {
			description = *rule.Description
		}

		_, err = tx.ExecContext(txCtx, `
			UPDATE event_rules
			SET rule_key = $1, definition = $2::jsonb, is_active = $3, repeat_able = $4, cooldown_sec = $5, description = $6
			WHERE event_rule_id = $7
		`, ruleKey, definition, isActive, repeatAble, cooldownSec, description, ruleID)

		if err != nil {
			return fmt.Errorf("failed to update event rule: %w", err)
		}

		return nil
	})
}

// DeleteEventRule deletes an event rule
func (c *Client) DeleteEventRule(ctx context.Context, ruleID string) error {
	org := orgFromContext(ctx)
	if org == "" {
		return fmt.Errorf("organization not found in context")
	}

	return c.WithOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
		query := `DELETE FROM event_rules WHERE event_rule_id = $1`

		result, err := tx.ExecContext(txCtx, query, ruleID)
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

// GetEventRulesWithAutomationInfo retrieves event rules with their associated automation info
func (c *Client) GetEventRulesWithAutomationInfo(ctx context.Context) ([]models.EventRule, error) {
	var rules []models.EventRule

	org := orgFromContext(ctx)
	if org == "" {
		return nil, fmt.Errorf("organization not found in context")
	}

	err := c.WithOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
		query := `
			SELECT er.event_rule_id, er.rule_key, er.definition, er.is_active, er.repeat_able, er.cooldown_sec, er.description, er.created_at,
				   a.id as automation_id, a.name as automation_name
			FROM event_rules er
			LEFT JOIN automations a ON a.event_rule_id = er.event_rule_id
			ORDER BY er.created_at DESC
		`

		rows, err := tx.QueryContext(txCtx, query)
		if err != nil {
			return fmt.Errorf("failed to query event rules: %w", err)
		}
		defer func() {
			_ = rows.Close()
		}()

		for rows.Next() {
			var rule models.EventRule
			var description sql.NullString
			var definition sql.NullString
			var cooldownSec sql.NullInt64
			var automationID sql.NullString
			var automationName sql.NullString

			err := rows.Scan(
				&rule.EventRuleID, &rule.RuleKey, &definition, &rule.IsActive,
				&rule.RepeatAble, &cooldownSec, &description, &rule.CreatedAt,
				&automationID, &automationName,
			)
			if err != nil {
				return err
			}

			if description.Valid {
				rule.Description = &description.String
			}
			if definition.Valid {
				rule.Definition = json.RawMessage(definition.String)
			}
			if cooldownSec.Valid {
				val := int(cooldownSec.Int64)
				rule.CooldownSec = &val
			}

			rules = append(rules, rule)
		}

		return rows.Err()
	})

	if err != nil {
		return nil, err
	}

	return rules, nil
}
