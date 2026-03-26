package timescaledb

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	apimodels "github.com/Space-DF/telemetry-service/internal/api/automations/models"
	"github.com/Space-DF/telemetry-service/internal/models"
	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/stephenafamo/bob"
)

// GetAutomations retrieves automations with pagination and optional filters
func (c *Client) GetAutomations(ctx context.Context, spaceID uuid.UUID, deviceID *string, statusList []bool, search string, limit, offset int) ([]models.AutomationWithActions, int, error) {
	var results []models.AutomationWithActions
	var total int

	org := orgFromContext(ctx)
	if org == "" {
		return nil, 0, fmt.Errorf("organization not found in context")
	}

	err := c.WithOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
		// Build WHERE clause dynamically
		argIndex := 1
		whereClause := fmt.Sprintf(" WHERE a.space_id = $%d", argIndex)
		args := []interface{}{spaceID}
		argIndex++

		if deviceID != nil {
			whereClause += fmt.Sprintf(" AND a.device_id = $%d", argIndex)
			args = append(args, *deviceID)
			argIndex++
		}

		if len(statusList) > 0 {
			whereClause += fmt.Sprintf(" AND er.is_active = ANY($%d)", argIndex)
			args = append(args, pq.Array(statusList))
			argIndex++
		}

		// Add search filter if provided
		if search != "" {
			if whereClause == "" {
				whereClause = " WHERE"
			} else {
				whereClause += " AND"
			}
			whereClause += fmt.Sprintf(" (a.name ILIKE $%d OR CAST(a.device_id AS VARCHAR) ILIKE $%d)", argIndex, argIndex+1)
			searchPattern := "%" + search + "%"
			args = append(args, searchPattern, searchPattern)
			argIndex += 2
		}

		// Count total
		countQuery := "SELECT COUNT(*) FROM automations a LEFT JOIN event_rules er ON er.event_rule_id = a.event_rule_id" + whereClause
		err := tx.QueryRowContext(txCtx, countQuery, args...).Scan(&total)
		if err != nil {
			return fmt.Errorf("failed to count automations: %w", err)
		}

		// Query automations with actions
		query := `
			SELECT a.id, a.name, a.device_id,
			       a.event_rule_id, a.space_id, a.updated_at, a.created_at,
			       er.event_rule_id, er.rule_key, er.definition, er.is_active, er.repeat_able, er.cooldown_sec, er.description,
			       COALESCE(
			         json_agg(
			           json_build_object(
			             'id', act.id,
			             'name', act.name,
			             'key', act.key,
			             'data', act.data::text,
			             'created_at', act.created_at
			           )
			         ) FILTER (WHERE act.id IS NOT NULL),
			         '[]'::json
			       ) as actions
			FROM automations a
			LEFT JOIN automation_actions aa ON aa.automation_id = a.id
			LEFT JOIN actions act ON act.id = aa.action_id
			LEFT JOIN event_rules er ON er.event_rule_id = a.event_rule_id
		` + whereClause + `
			GROUP BY a.id, a.name, a.device_id, a.event_rule_id, a.space_id, a.updated_at, a.created_at, er.event_rule_id, er.rule_key, er.definition::text, er.is_active, er.repeat_able, er.cooldown_sec, er.description
			ORDER BY a.created_at DESC
			LIMIT $` + fmt.Sprintf("%d", argIndex) + ` OFFSET $` + fmt.Sprintf("%d", argIndex+1)
		args = append(args, limit, offset)

		rows, err := tx.QueryContext(txCtx, query, args...)
		if err != nil {
			return fmt.Errorf("failed to query automations: %w", err)
		}
		defer func() { _ = rows.Close() }()

		for rows.Next() {
			var a models.Automation
			var actionsJSON json.RawMessage
			var eventRuleID, spaceID, erRuleKey, erDefinition, erEventRuleID, erDescription sql.NullString
			var erIsActive, erRepeatAble sql.NullBool
			var erCooldownSec sql.NullInt64

			if err := rows.Scan(
				&a.ID, &a.Name, &a.DeviceID,
				&eventRuleID, &spaceID, &a.UpdatedAt, &a.CreatedAt,
				&erEventRuleID, &erRuleKey, &erDefinition, &erIsActive, &erRepeatAble, &erCooldownSec, &erDescription,
				&actionsJSON,
			); err != nil {
				return err
			}

			if eventRuleID.Valid {
				a.EventRuleID = &eventRuleID.String
			}
			if spaceID.Valid {
				parsed, err := uuid.Parse(spaceID.String)
				if err == nil {
					a.SpaceID = &parsed
				}
			}

			// Build EventRule if we have data
			if erEventRuleID.Valid {
				var eventRule models.EventRule
				eventRule.EventRuleID = erEventRuleID.String
				if erRuleKey.Valid {
					eventRule.RuleKey = erRuleKey.String
				}
				if erDefinition.Valid {
					eventRule.Definition = &erDefinition.String
				}
				if erIsActive.Valid {
					eventRule.IsActive = &erIsActive.Bool
				}
				if erRepeatAble.Valid {
					eventRule.RepeatAble = &erRepeatAble.Bool
				}
				if erCooldownSec.Valid {
					cooldown := int(erCooldownSec.Int64)
					eventRule.CooldownSec = &cooldown
				}
				if erDescription.Valid {
					eventRule.Description = &erDescription.String
				}
				a.EventRule = &eventRule
			}

			result := models.AutomationWithActions{
				Automation: a,
			}

			// Parse actions from JSON
			if len(actionsJSON) > 0 && string(actionsJSON) != "null" {
				var actions []models.Action
				if err := json.Unmarshal(actionsJSON, &actions); err == nil {
					result.Actions = actions
				}
			}

			results = append(results, result)
		}

		return rows.Err()
	})

	if err != nil {
		return nil, 0, err
	}

	return results, total, nil
}

// GetAutomationByID retrieves a single automation by ID
func (c *Client) GetAutomationByID(ctx context.Context, automationID string) (*models.AutomationWithActions, error) {
	if automationID == "" {
		return nil, fmt.Errorf("automation_id is required")
	}

	org := orgFromContext(ctx)
	if org == "" {
		return nil, fmt.Errorf("organization not found in context")
	}

	var result models.AutomationWithActions

	err := c.WithOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
		query := `
			SELECT a.id, a.name, a.device_id,
			       a.event_rule_id, a.space_id, a.updated_at, a.created_at,
			       er.event_rule_id, er.rule_key, er.definition, er.is_active, er.repeat_able, er.cooldown_sec, er.description,
			       COALESCE(
			         json_agg(
			           json_build_object(
			             'id', act.id,
			             'name', act.name,
			             'key', act.key,
			             'data', act.data::text,
			             'created_at', act.created_at
			           )
			         ) FILTER (WHERE act.id IS NOT NULL),
			         '[]'::json
			       ) as actions
			FROM automations a
			LEFT JOIN automation_actions aa ON aa.automation_id = a.id
			LEFT JOIN actions act ON act.id = aa.action_id
			LEFT JOIN event_rules er ON er.event_rule_id = a.event_rule_id
			WHERE a.id = $1
			GROUP BY a.id, a.name, a.device_id, a.event_rule_id, a.space_id, a.updated_at, a.created_at, er.event_rule_id, er.rule_key, er.definition::text, er.is_active, er.repeat_able, er.cooldown_sec, er.description
		`
		var actionsJSON json.RawMessage
		var eventRuleID, spaceID, erEventRuleID, erRuleKey, erDefinition, erDescription sql.NullString
		var erIsActive, erRepeatAble sql.NullBool
		var erCooldownSec sql.NullInt64

		err := tx.QueryRowContext(txCtx, query, automationID).Scan(
			&result.ID, &result.Name, &result.DeviceID,
			&eventRuleID, &spaceID, &result.UpdatedAt, &result.CreatedAt,
			&erEventRuleID, &erRuleKey, &erDefinition, &erIsActive, &erRepeatAble, &erCooldownSec, &erDescription,
			&actionsJSON,
		)

		if err != nil {
			if err == sql.ErrNoRows {
				return fmt.Errorf("automation not found")
			}
			return fmt.Errorf("failed to query automation: %w", err)
		}

		if eventRuleID.Valid {
			result.EventRuleID = &eventRuleID.String
		}
		if spaceID.Valid {
			parsed, err := uuid.Parse(spaceID.String)
			if err == nil {
				result.SpaceID = &parsed
			}
		}

		// Build EventRule if we have data
		if erEventRuleID.Valid {
			var eventRule models.EventRule
			eventRule.EventRuleID = erEventRuleID.String
			if erRuleKey.Valid {
				eventRule.RuleKey = erRuleKey.String
			}
			if erDefinition.Valid {
				eventRule.Definition = &erDefinition.String
			}
			if erIsActive.Valid {
				eventRule.IsActive = &erIsActive.Bool
			}
			if erRepeatAble.Valid {
				eventRule.RepeatAble = &erRepeatAble.Bool
			}
			if erCooldownSec.Valid {
				cooldown := int(erCooldownSec.Int64)
				eventRule.CooldownSec = &cooldown
			}
			if erDescription.Valid {
				eventRule.Description = &erDescription.String
			}
			result.EventRule = &eventRule
		}

		// Parse actions from JSON
		if len(actionsJSON) > 0 && string(actionsJSON) != "null" {
			var actions []models.Action
			if err := json.Unmarshal(actionsJSON, &actions); err == nil {
				result.Actions = actions
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return &result, nil
}

// CreateAutomation creates a new automation with an associated event rule
func (c *Client) CreateAutomation(ctx context.Context, req *apimodels.AutomationRequest) (*models.AutomationWithActions, error) {
	if req == nil {
		return nil, fmt.Errorf("nil request")
	}

	org := orgFromContext(ctx)
	if org == "" {
		return nil, fmt.Errorf("organization not found in context")
	}

	var result models.AutomationWithActions

	err := c.WithOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
		// Validate that all action_ids exist
		for _, actionID := range req.ActionIDs {
			var exists bool
			err := tx.QueryRowContext(txCtx, "SELECT EXISTS(SELECT 1 FROM actions WHERE id = $1)", actionID).Scan(&exists)
			if err != nil {
				return fmt.Errorf("failed to validate action_id: %w", err)
			}
			if !exists {
				return fmt.Errorf("action with id %s does not exist", actionID)
			}
		}

		// Create event rule for the automation
		ruleKey := "automation"
		isActive := true
		repeatAble := true
		cooldownSec := 0
		description := *req.Name
		definition := "{}"

		if req.EventRule != nil {
			if req.EventRule.RuleKey != nil && *req.EventRule.RuleKey != "" {
				ruleKey = *req.EventRule.RuleKey
			}
			if req.EventRule.IsActive != nil {
				isActive = *req.EventRule.IsActive
			}
			if req.EventRule.RepeatAble != nil {
				repeatAble = *req.EventRule.RepeatAble
			}
			if req.EventRule.CooldownSec != nil {
				cooldownSec = *req.EventRule.CooldownSec
			}
			if len(req.EventRule.Definition) > 0 {
				// Parse the RawMessage as JSON to ensure it's stored as a proper JSON object
				var defObj interface{}
				if err := json.Unmarshal(req.EventRule.Definition, &defObj); err == nil {
					defBytes, _ := json.Marshal(defObj)
					definition = string(defBytes)
				} else {
					definition = string(req.EventRule.Definition)
				}
			}
			if req.EventRule.Description != nil && *req.EventRule.Description != "" {
				description = *req.EventRule.Description
			}
		}

		var eventRuleID uuid.UUID
		err := tx.QueryRowContext(txCtx, `
			INSERT INTO event_rules (rule_key, definition, is_active, repeat_able, cooldown_sec, description)
			VALUES ($1, $2::jsonb, $3, $4, $5, $6)
			RETURNING event_rule_id
		`, ruleKey, definition, isActive, repeatAble, cooldownSec, description).Scan(&eventRuleID)
		if err != nil {
			return fmt.Errorf("failed to create event rule: %w", err)
		}

		// Insert automation
		var spaceID sql.NullString
		if req.SpaceID != nil {
			spaceID.Valid = true
			spaceID.String = req.SpaceID.String()
		}

		eventRuleIDStr := eventRuleID.String()
		var returnedSpaceID sql.NullString
		err = tx.QueryRowContext(txCtx, `
			INSERT INTO automations (name, device_id, event_rule_id, space_id)
			VALUES ($1, $2, $3, $4)
			RETURNING id, space_id, updated_at, created_at
		`, req.Name, req.DeviceID, eventRuleID, spaceID).Scan(
			&result.ID, &returnedSpaceID, &result.UpdatedAt, &result.CreatedAt,
		)

		if err != nil {
			return fmt.Errorf("failed to insert automation: %w", err)
		}

		result.Name = *req.Name
		result.DeviceID = req.DeviceID
		result.EventRuleID = &eventRuleIDStr
		if returnedSpaceID.Valid {
			parsed, err := uuid.Parse(returnedSpaceID.String)
			if err == nil {
				result.SpaceID = &parsed
			}
		}
		for _, actionID := range req.ActionIDs {
			_, err := tx.ExecContext(txCtx, `
				INSERT INTO automation_actions (automation_id, action_id)
				VALUES ($1, $2)
			`, result.ID, actionID)
			if err != nil {
				return fmt.Errorf("failed to insert automation_action: %w", err)
			}
		}

		// Fetch associated actions
		if len(req.ActionIDs) > 0 {
			actions, err := c.getActionsByIDs(txCtx, tx, req.ActionIDs)
			if err == nil {
				result.Actions = actions
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return &result, nil
}

// UpdateAutomation updates an existing automation
func (c *Client) UpdateAutomation(ctx context.Context, automationID string, req *apimodels.AutomationRequest) (*models.AutomationWithActions, error) {
	if req == nil {
		return nil, fmt.Errorf("nil request")
	}
	if automationID == "" {
		return nil, fmt.Errorf("automation_id is required")
	}

	org := orgFromContext(ctx)
	if org == "" {
		return nil, fmt.Errorf("organization not found in context")
	}

	var result models.AutomationWithActions

	err := c.WithOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
		// Validate that all action_ids exist
		for _, actionID := range req.ActionIDs {
			var exists bool
			err := tx.QueryRowContext(txCtx, "SELECT EXISTS(SELECT 1 FROM actions WHERE id = $1)", actionID).Scan(&exists)
			if err != nil {
				return fmt.Errorf("failed to validate action_id: %w", err)
			}
			if !exists {
				return fmt.Errorf("action with id %s does not exist", actionID)
			}
		}

		// Get current event_rule_id
		var currentEventRuleID sql.NullString
		err := tx.QueryRowContext(txCtx, "SELECT event_rule_id FROM automations WHERE id = $1", automationID).Scan(&currentEventRuleID)
		if err != nil {
			if err == sql.ErrNoRows {
				return fmt.Errorf("automation not found")
			}
			return fmt.Errorf("failed to get automation: %w", err)
		}

		// Update the event rule with all fields
		description := *req.Name
		ruleKey := "automation"
		definition := "{}"
		isActive := true
		repeatAble := true
		cooldownSec := 0

		if req.EventRule != nil {
			if req.EventRule.RuleKey != nil && *req.EventRule.RuleKey != "" {
				ruleKey = *req.EventRule.RuleKey
			}
			if len(req.EventRule.Definition) > 0 {
				// Parse the RawMessage as JSON to ensure it's stored as a proper JSON object
				var defObj interface{}
				if err := json.Unmarshal(req.EventRule.Definition, &defObj); err == nil {
					defBytes, _ := json.Marshal(defObj)
					definition = string(defBytes)
				} else {
					definition = string(req.EventRule.Definition)
				}
			}
			if req.EventRule.IsActive != nil {
				isActive = *req.EventRule.IsActive
			}
			if req.EventRule.RepeatAble != nil {
				repeatAble = *req.EventRule.RepeatAble
			}
			if req.EventRule.CooldownSec != nil {
				cooldownSec = *req.EventRule.CooldownSec
			}
			if req.EventRule.Description != nil && *req.EventRule.Description != "" {
				description = *req.EventRule.Description
			}
		}

		_, err = tx.ExecContext(txCtx, `
			UPDATE event_rules
			SET rule_key = $1, definition = $2::jsonb, is_active = $3, repeat_able = $4, cooldown_sec = $5, description = $6
			WHERE event_rule_id = $7
		`, ruleKey, definition, isActive, repeatAble, cooldownSec, description, currentEventRuleID)
		if err != nil {
			return fmt.Errorf("failed to update event rule: %w", err)
		}

		var eventRuleIDStr sql.NullString
		err = tx.QueryRowContext(txCtx, `
			UPDATE automations
			SET name = $1, device_id = $2, updated_at = NOW()
			WHERE id = $3
			RETURNING id, event_rule_id, updated_at, created_at
		`, req.Name, req.DeviceID, automationID).Scan(
			&result.ID, &eventRuleIDStr, &result.UpdatedAt, &result.CreatedAt,
		)

		if err != nil {
			if err == sql.ErrNoRows {
				return fmt.Errorf("automation not found")
			}
			return fmt.Errorf("failed to update automation: %w", err)
		}

		result.Name = *req.Name
		result.DeviceID = req.DeviceID
		if eventRuleIDStr.Valid {
			result.EventRuleID = &eventRuleIDStr.String
		}

		// Delete existing automation_actions and insert new ones
		_, err = tx.ExecContext(txCtx, `DELETE FROM automation_actions WHERE automation_id = $1`, result.ID)
		if err != nil {
			return fmt.Errorf("failed to delete old automation_actions: %w", err)
		}

		for _, actionID := range req.ActionIDs {
			_, err := tx.ExecContext(txCtx, `
				INSERT INTO automation_actions (automation_id, action_id)
				VALUES ($1, $2)
			`, result.ID, actionID)
			if err != nil {
				return fmt.Errorf("failed to insert automation_action: %w", err)
			}
		}

		// Fetch associated actions
		if len(req.ActionIDs) > 0 {
			actions, err := c.getActionsByIDs(txCtx, tx, req.ActionIDs)
			if err == nil {
				result.Actions = actions
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return &result, nil
}

// DeleteAutomation deletes an automation
func (c *Client) DeleteAutomation(ctx context.Context, automationID string) error {
	if automationID == "" {
		return fmt.Errorf("automation_id is required")
	}

	org := orgFromContext(ctx)
	if org == "" {
		return fmt.Errorf("organization not found in context")
	}

	return c.WithOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
		var eventRuleID string

		// Delete automation and get event rule ID to delete associated event rule
		err := tx.QueryRowContext(txCtx, `
			DELETE FROM automations
			WHERE id = $1
			RETURNING event_rule_id
		`, automationID).Scan(&eventRuleID)

		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("automation not found")
			}
			return fmt.Errorf("failed to delete automation: %w", err)
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

// GetActions retrieves all actions with pagination
func (c *Client) GetActions(ctx context.Context, search string, limit, offset int) ([]models.Action, int, error) {
	var actions []models.Action
	var total int

	org := orgFromContext(ctx)
	if org == "" {
		return nil, 0, fmt.Errorf("organization not found in context")
	}

	err := c.WithOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
		// Build WHERE clause
		whereClause := ""
		args := []interface{}{}

		if search != "" {
			whereClause = " WHERE name ILIKE $1 OR key ILIKE $1"
			args = append(args, "%"+search+"%")
		}

		// Count total
		countQuery := "SELECT COUNT(*) FROM actions" + whereClause
		err := tx.QueryRowContext(txCtx, countQuery, args...).Scan(&total)
		if err != nil {
			return fmt.Errorf("failed to count actions: %w", err)
		}

		// Query actions
		argIndex := len(args) + 1
		query := `
			SELECT id, name, key, data::text, created_at
			FROM actions
		` + whereClause + ` ORDER BY created_at DESC LIMIT $` + fmt.Sprintf("%d", argIndex) + ` OFFSET $` + fmt.Sprintf("%d", argIndex+1)
		args = append(args, limit, offset)

		rows, err := tx.QueryContext(txCtx, query, args...)
		if err != nil {
			return fmt.Errorf("failed to query actions: %w", err)
		}
		defer func() { _ = rows.Close() }()

		for rows.Next() {
			var a models.Action
			var data sql.NullString

			if err := rows.Scan(&a.ID, &a.Name, &a.Key, &data, &a.CreatedAt); err != nil {
				return err
			}

			if data.Valid {
				a.Data = &data.String
			}

			actions = append(actions, a)
		}

		return rows.Err()
	})

	if err != nil {
		return nil, 0, err
	}

	return actions, total, nil
}

// CreateAction creates a new action
func (c *Client) CreateAction(ctx context.Context, name, key string, data map[string]interface{}) (*models.Action, error) {
	org := orgFromContext(ctx)
	if org == "" {
		return nil, fmt.Errorf("organization not found in context")
	}

	var action models.Action

	err := c.WithOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
		var dataJSON *string
		if data != nil {
			raw, err := json.Marshal(data)
			if err != nil {
				return fmt.Errorf("failed to marshal action data: %w", err)
			}
			dataStr := string(raw)
			dataJSON = &dataStr
		}

		err := tx.QueryRowContext(txCtx, `
			INSERT INTO actions (name, key, data)
			VALUES ($1, $2, $3::jsonb)
			RETURNING id, name, key, data::text, created_at
		`, name, key, dataJSON).Scan(
			&action.ID, &action.Name, &action.Key, &action.Data, &action.CreatedAt,
		)

		if err != nil {
			return fmt.Errorf("failed to insert action: %w", err)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return &action, nil
}

// UpdateAction updates an existing action
func (c *Client) UpdateAction(ctx context.Context, actionID, name, key string, data map[string]interface{}) (*models.Action, error) {
	if actionID == "" {
		return nil, fmt.Errorf("action_id is required")
	}

	org := orgFromContext(ctx)
	if org == "" {
		return nil, fmt.Errorf("organization not found in context")
	}

	var action models.Action

	err := c.WithOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
		var dataJSON *string
		if data != nil {
			raw, err := json.Marshal(data)
			if err != nil {
				return fmt.Errorf("failed to marshal action data: %w", err)
			}
			dataStr := string(raw)
			dataJSON = &dataStr
		}

		err := tx.QueryRowContext(txCtx, `
			UPDATE actions
			SET name = $1, key = $2, data = $3::jsonb
			WHERE id = $4
			RETURNING id, name, key, data::text, created_at
		`, name, key, dataJSON, actionID).Scan(
			&action.ID, &action.Name, &action.Key, &action.Data, &action.CreatedAt,
		)

		if err != nil {
			if err == sql.ErrNoRows {
				return fmt.Errorf("action not found")
			}
			return fmt.Errorf("failed to update action: %w", err)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return &action, nil
}

// DeleteAction deletes an action
func (c *Client) DeleteAction(ctx context.Context, actionID string) error {
	if actionID == "" {
		return fmt.Errorf("action_id is required")
	}

	org := orgFromContext(ctx)
	if org == "" {
		return fmt.Errorf("organization not found in context")
	}

	return c.WithOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
		result, err := tx.ExecContext(txCtx, `DELETE FROM actions WHERE id = $1`, actionID)
		if err != nil {
			return fmt.Errorf("failed to delete action: %w", err)
		}

		rows, _ := result.RowsAffected()
		if rows == 0 {
			return fmt.Errorf("action not found")
		}

		return nil
	})
}

// getActionsByIDs retrieves actions by their IDs and appends them to the result
func (c *Client) getActionsByIDs(ctx context.Context, tx bob.Tx, actionIDs []string) ([]models.Action, error) {
	if len(actionIDs) == 0 {
		return nil, nil
	}

	var actions []models.Action

	query := `SELECT id, name, key, data::text, created_at FROM actions WHERE id = ANY($1)`
	rows, err := tx.QueryContext(ctx, query, pq.Array(actionIDs))
	if err != nil {
		return nil, fmt.Errorf("failed to query actions: %w", err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var a models.Action
		var data sql.NullString

		if err := rows.Scan(&a.ID, &a.Name, &a.Key, &data, &a.CreatedAt); err != nil {
			return nil, err
		}

		if data.Valid {
			a.Data = &data.String
		}

		actions = append(actions, a)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return actions, nil
}
