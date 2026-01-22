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
	"github.com/stephenafamo/bob"
)

// EventType constants
const (
	EventTypeStateChanged    = "state_changed"
	EventTypeServiceCall     = "service_call"
	EventTypeAutomation      = "automation_triggered"
	EventTypeDeviceTriggered = "device_triggered"
)

// Pagination constants
const (
	DefaultPage          = 1
	DefaultPageSize      = 20
	MaxPageSize          = 100
	DefaultEventLimit    = 100
)

// getOrCreateEventTypeID retrieves the event_type_id for a given event type,
// creating it if it doesn't exist.
func (c *Client) getOrCreateEventTypeID(ctx context.Context, tx bob.Tx, eventType string) (int, error) {
	var eventTypeID int
	err := tx.QueryRowContext(ctx, `
		SELECT event_type_id FROM event_types WHERE event_type = $1
	`, eventType).Scan(&eventTypeID)

	if err == sql.ErrNoRows {
		// Create new event type
		err = tx.QueryRowContext(ctx, `
			INSERT INTO event_types (event_type) VALUES ($1)
			RETURNING event_type_id
		`, eventType).Scan(&eventTypeID)
	}

	if err != nil {
		return 0, fmt.Errorf("failed to get or create event type '%s': %w", eventType, err)
	}

	return eventTypeID, nil
}

// getOrCreateEventDataID stores event data and returns its ID,
// reusing existing data if the hash matches.
func (c *Client) getOrCreateEventDataID(ctx context.Context, tx bob.Tx, data map[string]interface{}) (*int64, error) {
	if len(data) == 0 {
		return nil, nil
	}

	rawData, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal event data: %w", err)
	}

	hash := int64(crc32.ChecksumIEEE(rawData))

	var dataID int64
	err = tx.QueryRowContext(ctx, `
		INSERT INTO event_data (hash, shared_data)
		VALUES ($1, $2)
		ON CONFLICT (hash) DO UPDATE SET shared_data = EXCLUDED.shared_data
		RETURNING data_id
	`, hash, rawData).Scan(&dataID)

	if err != nil {
		return nil, fmt.Errorf("failed to insert event data: %w", err)
	}

	return &dataID, nil
}

// getOrCreateStateAttributesID stores state attributes and returns its ID,
// reusing existing attributes if the hash matches.
func (c *Client) getOrCreateStateAttributesID(ctx context.Context, tx bob.Tx, attrs map[string]interface{}) (*int, error) {
	if len(attrs) == 0 {
		return nil, nil
	}

	rawAttrs, err := json.Marshal(attrs)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal state attributes: %w", err)
	}

	hash := int64(crc32.ChecksumIEEE(rawAttrs))

	var attributesID int
	err = tx.QueryRowContext(ctx, `
		INSERT INTO state_attributes (hash, shared_attrs)
		VALUES ($1, $2)
		ON CONFLICT (hash) DO UPDATE SET shared_attrs = EXCLUDED.shared_attrs
		RETURNING attributes_id
	`, hash, rawAttrs).Scan(&attributesID)

	if err != nil {
		return nil, fmt.Errorf("failed to insert state attributes: %w", err)
	}

	return &attributesID, nil
}

// getOrCreateStatesMetaID retrieves or creates the metadata_id for an entity.
func (c *Client) getOrCreateStatesMetaID(ctx context.Context, tx bob.Tx, entityID string) (int, error) {
	var metadataID int
	err := tx.QueryRowContext(ctx, `
		SELECT metadata_id FROM states_meta WHERE entity_id = $1
	`, entityID).Scan(&metadataID)

	if err == sql.ErrNoRows {
		// Create new metadata
		err = tx.QueryRowContext(ctx, `
			INSERT INTO states_meta (entity_id) VALUES ($1)
			RETURNING metadata_id
		`, entityID).Scan(&metadataID)
	}

	if err != nil {
		return 0, fmt.Errorf("failed to get or create states meta for '%s': %w", entityID, err)
	}

	return metadataID, nil
}

// GetEventsByDevice retrieves all events for a specific entity.
func (c *Client) GetEventsByDevice(ctx context.Context, org, deviceID string, limit int) ([]models.Event, error) {
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
		// Query events where the event_data contains this device_id
		query := `
			SELECT e.event_id, e.event_type_id, e.data_id, e.space_slug, e.context_id_bin,
				   e.trigger_id, e.allow_new_event, e.time_fired_ts, et.event_type, ed.shared_data
			FROM events e
			JOIN event_types et ON e.event_type_id = et.event_type_id
			LEFT JOIN event_data ed ON e.data_id = ed.data_id
			WHERE ed.shared_data->>'device_id' = $1
			ORDER BY e.time_fired_ts DESC
			LIMIT $2
		`

		rows, err := tx.QueryContext(txCtx, query, deviceID, limit)
		if err != nil {
			return fmt.Errorf("failed to query events by device: %w", err)
		}
		defer func(){
			_ = rows.Close()
		}()

		for rows.Next() {
			var e models.Event
			var dataID sql.NullInt64
			var slug sql.NullString
			var contextID []byte
			var triggerID sql.NullString
			var allowNewEvent sql.NullBool
			var sharedData []byte

			if err := rows.Scan(&e.EventID, &e.EventTypeID, &dataID, &slug, &contextID, &triggerID, &allowNewEvent, &e.TimeFiredTs, &e.EventType, &sharedData); err != nil {
				return err
			}

			if dataID.Valid {
				e.DataID = &dataID.Int64
			}
			if slug.Valid {
				e.SpaceSlug = slug.String
			}
			if len(contextID) > 0 {
				e.ContextID = contextID
			}
			if triggerID.Valid {
				e.TriggerID = &triggerID.String
			}
			if allowNewEvent.Valid {
				e.AllowNewEvent = &allowNewEvent.Bool
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

// ============================================================================
// Event Rules
// ============================================================================

// populateEventRuleResponse populates an EventRuleResponse from request data and times
func populateEventRuleResponse(result *models.EventRuleResponse, req *models.EventRuleRequest, startTime, endTime *time.Time) {
	if req.EntityID != nil {
		result.EntityID = req.EntityID
	}
	if req.DeviceModelID != nil {
		result.DeviceModelID = req.DeviceModelID
	}
	if req.RuleKey != nil {
		result.RuleKey = req.RuleKey
	}
	if req.Operator != nil {
		result.Operator = req.Operator
	}
	result.Operand = req.Operand
	if req.Status != nil {
		result.Status = req.Status
	}
	if req.IsActive != nil {
		result.IsActive = req.IsActive
	}
	result.StartTime = startTime
	result.EndTime = endTime
}

// GetEventRules retrieves event rules with pagination
func (c *Client) GetEventRules(ctx context.Context, entityID string, page, pageSize int) ([]models.EventRule, int, error) {
	if page <= 0 {
		page = DefaultPage
	}
	if pageSize <= 0 || pageSize > MaxPageSize {
		pageSize = DefaultPageSize
	}

	offset := (page - 1) * pageSize

	var rules []models.EventRule
	var total int

	err := c.WithOrgTx(ctx, "", func(txCtx context.Context, tx bob.Tx) error {
		// Count total
		countQuery := `SELECT COUNT(*) FROM event_rules`
		args := []interface{}{}

		whereClause := ""
		if entityID != "" {
			whereClause = " WHERE entity_id = $1"
			args = append(args, entityID)
		}

		countQuery += whereClause
		err := tx.QueryRowContext(txCtx, countQuery, args...).Scan(&total)
		if err != nil {
			return fmt.Errorf("failed to count event rules: %w", err)
		}

		// Query rules
		query := `
			SELECT er.event_rule_id, er.entity_id, er.device_model_id,
				   er.rule_key, er.operator, er.operand, er.status, er.is_active,
				   er.start_time, er.end_time, er.created_at, er.updated_at
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
				&r.EventRuleID, &r.EntityID, &r.DeviceModelID,
				&r.RuleKey, &r.Operator, &r.Operand, &r.Status, &r.IsActive,
				&r.StartTime, &r.EndTime, &r.CreatedAt, &r.UpdatedAt,
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

// CreateEventRule creates a new event rule
func (c *Client) CreateEventRule(ctx context.Context, req *models.EventRuleRequest) (*models.EventRuleResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("nil request")
	}

	var result models.EventRuleResponse

	err := c.WithOrgTx(ctx, "", func(txCtx context.Context, tx bob.Tx) error {
		// Parse start and end times
		var startTime, endTime *time.Time
		if req.StartTime != nil {
			t, parseErr := time.Parse(time.RFC3339, *req.StartTime)
			if parseErr != nil {
				return fmt.Errorf("invalid start_time format: %w", parseErr)
			}
			startTime = &t
		}
		if req.EndTime != nil {
			t, parseErr := time.Parse(time.RFC3339, *req.EndTime)
			if parseErr != nil {
				return fmt.Errorf("invalid end_time format: %w", parseErr)
			}
			endTime = &t
		}

		// Insert event rule
		err := tx.QueryRowContext(txCtx, `
			INSERT INTO event_rules (entity_id, device_model_id, rule_key, operator, operand, status, is_active, start_time, end_time)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
			RETURNING event_rule_id, created_at, updated_at
		`, req.EntityID, req.DeviceModelID, req.RuleKey, req.Operator, req.Operand,
			req.Status, req.IsActive, startTime, endTime).Scan(
			&result.EventRuleID, &result.CreatedAt, &result.UpdatedAt,
		)

		if err != nil {
			return fmt.Errorf("failed to insert event rule: %w", err)
		}
		populateEventRuleResponse(&result, req, startTime, endTime)

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

	err := c.WithOrgTx(ctx, "", func(txCtx context.Context, tx bob.Tx) error {
		// Parse start and end times
		var startTime, endTime *time.Time
		if req.StartTime != nil {
			t, parseErr := time.Parse(time.RFC3339, *req.StartTime)
			if parseErr != nil {
				return fmt.Errorf("invalid start_time format: %w", parseErr)
			}
			startTime = &t
		}
		if req.EndTime != nil {
			t, parseErr := time.Parse(time.RFC3339, *req.EndTime)
			if parseErr != nil {
				return fmt.Errorf("invalid end_time format: %w", parseErr)
			}
			endTime = &t
		}

		// Update event rule
		err := tx.QueryRowContext(txCtx, `
			UPDATE event_rules
			SET entity_id = $1, device_model_id = $2, rule_key = $3,
			    operator = $4, operand = $5, status = $6, is_active = $7,
			    start_time = $8, end_time = $9, updated_at = NOW()
			WHERE event_rule_id = $10
			RETURNING event_rule_id, created_at, updated_at
		`, req.EntityID, req.DeviceModelID, req.RuleKey, req.Operator, req.Operand,
			req.Status, req.IsActive, startTime, endTime, ruleID).Scan(
			&result.EventRuleID, &result.CreatedAt, &result.UpdatedAt,
		)

		if err != nil {
			return fmt.Errorf("failed to update event rule: %w", err)
		}
		populateEventRuleResponse(&result, req, startTime, endTime)

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

	return c.WithOrgTx(ctx, "", func(txCtx context.Context, tx bob.Tx) error {
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

// ruleConfig represents a single event rule configuration for seeding
type ruleConfig struct {
	RuleKey         string
	EntityIDPattern string
	Operator        string
	Operand         string
	EventType       string
	EventLevel      string
	Description     string
	Status          string
	IsActive        bool
}

// deviceModelRules represents event rules for a specific device model
type deviceModelRules struct {
	DeviceModel   string
	DeviceModelID string
	Rules         []ruleConfig
}

// SeedDefaultEventRules seeds default event rules from configuration
// This is typically called on service startup to ensure default rules exist
func (c *Client) SeedDefaultEventRules(ctx context.Context, rulesConfig interface{}) error {
	var config struct {
		DeviceModels []deviceModelRules
	}

	// Use type assertion to handle different config types
	// This allows passing either the raw YAML unmarshalled struct or our custom config type
	switch cfg := rulesConfig.(type) {
	case map[string]interface{}:
		// Handle raw YAML map
		if dms, ok := cfg["device_models"].([]interface{}); ok {
			for _, dm := range dms {
				dmMap, ok := dm.(map[string]interface{})
				if !ok {
					continue
				}
				dmr := deviceModelRules{
					DeviceModel:   getString(dmMap, "device_model"),
					DeviceModelID: getString(dmMap, "device_model_id"),
				}
				if rules, ok := dmMap["rules"].([]interface{}); ok {
					for _, r := range rules {
						rMap, ok := r.(map[string]interface{})
						if !ok {
							continue
						}
						dmr.Rules = append(dmr.Rules, ruleConfig{
							RuleKey:         getString(rMap, "rule_key"),
							EntityIDPattern: getString(rMap, "entity_id_pattern"),
							Operator:        getString(rMap, "operator"),
							Operand:         getString(rMap, "operand"),
							EventType:       getString(rMap, "event_type"),
							EventLevel:      getString(rMap, "event_level"),
							Description:     getString(rMap, "description"),
							Status:          getString(rMap, "status"),
							IsActive:        getBool(rMap, "is_active"),
						})
					}
				}
				config.DeviceModels = append(config.DeviceModels, dmr)
			}
		}
	default:
		return fmt.Errorf("unsupported config type: %T", rulesConfig)
	}

	// Seed rules for each device model
	for _, dm := range config.DeviceModels {
		if err := c.seedDeviceModelRules(ctx, dm); err != nil {
			return fmt.Errorf("failed to seed rules for device model %s: %w", dm.DeviceModel, err)
		}
	}

	return nil
}

// seedDeviceModelRules seeds event rules for a specific device model
func (c *Client) seedDeviceModelRules(ctx context.Context, dm deviceModelRules) error {
	return c.WithOrgTx(ctx, "", func(txCtx context.Context, tx bob.Tx) error {
		for _, ruleCfg := range dm.Rules {
			// Check if rule already exists (by device_model_id + rule_key + operator + operand)
			var existingRuleID string
			checkQuery := `
				SELECT event_rule_id FROM event_rules
				WHERE device_model_id = $1 AND rule_key = $2 AND operator = $3 AND operand = $4
				LIMIT 1
			`
			err := tx.QueryRowContext(txCtx, checkQuery, dm.DeviceModelID, ruleCfg.RuleKey, ruleCfg.Operator, ruleCfg.Operand).Scan(&existingRuleID)

			if err == nil {
				// Rule already exists, skip
				log.Printf("[EventRules] Rule already exists for %s:%s (%s %s), skipping", dm.DeviceModel, ruleCfg.RuleKey, ruleCfg.Operator, ruleCfg.Operand)
				continue
			}

			if err != sql.ErrNoRows {
				return fmt.Errorf("failed to check existing rule: %w", err)
			}

			// Create new rule
			var ruleID string
			var entityID *string
			if ruleCfg.EntityIDPattern != "" {
				entityID = &ruleCfg.EntityIDPattern
			}
			ruleKey := &ruleCfg.RuleKey
			operator := &ruleCfg.Operator
			status := &ruleCfg.Status
			isActive := &ruleCfg.IsActive

			insertQuery := `
				INSERT INTO event_rules (device_model_id, entity_id, rule_key, operator, operand, status, is_active)
				VALUES ($1, $2, $3, $4, $5, $6, $7)
				RETURNING event_rule_id
			`
			err = tx.QueryRowContext(txCtx, insertQuery,
				dm.DeviceModelID, entityID, ruleKey, operator, ruleCfg.Operand, status, isActive,
			).Scan(&ruleID)

			if err != nil {
				return fmt.Errorf("failed to insert event rule: %w", err)
			}

			log.Printf("[EventRules] Seeded rule: %s - %s %s %s", dm.DeviceModel, ruleCfg.RuleKey, ruleCfg.Operator, ruleCfg.Operand)
		}
		return nil
	})
}

// getString safely extracts a string value from a map
func getString(m map[string]interface{}, key string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

// getBool safely extracts a bool value from a map
func getBool(m map[string]interface{}, key string) bool {
	if v, ok := m[key]; ok {
		if b, ok := v.(bool); ok {
			return b
		}
	}
	return false
}
