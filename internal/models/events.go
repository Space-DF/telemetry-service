package models

import (
	"encoding/json"
	"time"
)

// EventType represents a type of event (e.g., "state_changed", "service_call")
type EventType struct {
	EventTypeID int       `json:"event_type_id" db:"event_type_id"`
	EventType   string    `json:"event_type" db:"event_type"`
	CreatedAt   time.Time `json:"created_at" db:"created_at"`
}

// EventData represents shared event data (deduplicated by hash)
type EventData struct {
	DataID     int64           `json:"data_id" db:"data_id"`
	Hash       int64           `json:"hash" db:"hash"`
	SharedData json.RawMessage `json:"shared_data" db:"shared_data"`
	CreatedAt  time.Time       `json:"created_at" db:"created_at"`
}

// EventRule represents an automation rule for triggering events based on conditions
type EventRule struct {
	EventRuleID   string     `json:"event_rule_id" db:"event_rule_id"`
	DeviceID      *string    `json:"device_id,omitempty" db:"device_id"`
	RuleKey       *string    `json:"rule_key,omitempty" db:"rule_key"` // e.g., 'battery_low', 'temperature_low'
	Operator      *string    `json:"operator,omitempty" db:"operator"`   // eq, ne, gt, lt, gte, lte, contains
	Operand       string     `json:"operand" db:"operand"`
	Status        *string    `json:"status,omitempty" db:"status"` // active, inactive, paused
	IsActive      *bool      `json:"is_active,omitempty" db:"is_active"`
	StartTime     *time.Time `json:"start_time,omitempty" db:"start_time"`
	EndTime       *time.Time `json:"end_time,omitempty" db:"end_time"`
	AllowNewEvent *bool      `json:"allow_new_event,omitempty" db:"allow_new_event"`
	CreatedAt     time.Time  `json:"created_at" db:"created_at"`
	UpdatedAt     time.Time  `json:"updated_at" db:"updated_at"`
}

// EventRuleRequest represents a request to create or update an event rule
type EventRuleRequest struct {
	DeviceID      *string `json:"device_id,omitempty" validate:"required,uuid"`
	RuleKey       *string `json:"rule_key,omitempty" validate:"required"`
	Operator      *string `json:"operator,omitempty" validate:"omitempty,oneof=eq ne gt lt gte lte contains"`
	Operand       string  `json:"operand" validate:"required"`
	Status        *string `json:"status,omitempty" validate:"omitempty,oneof=active inactive paused"`
	IsActive      *bool   `json:"is_active,omitempty"`
	AllowNewEvent *bool   `json:"allow_new_event,omitempty"`
	StartTime     *string `json:"start_time,omitempty" validate:"omitempty,datetime=2006-01-02T15:04:05Z07:00"`
	EndTime       *string `json:"end_time,omitempty" validate:"omitempty,datetime=2006-01-02T15:04:05Z07:00"`
}

// EventRuleResponse represents an event rule response
type EventRuleResponse struct {
	EventRuleID string     `json:"event_rule_id"`
	DeviceID    *string    `json:"device_id,omitempty"`
	RuleKey     *string    `json:"rule_key,omitempty"`
	Operator    *string    `json:"operator,omitempty"`
	Operand     string     `json:"operand"`
	Status      *string    `json:"status,omitempty"`
	IsActive    *bool      `json:"is_active,omitempty"`
	StartTime   *time.Time `json:"start_time,omitempty"`
	EndTime     *time.Time `json:"end_time,omitempty"`
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
}

// EventRulesListResponse represents a paginated list of event rules
type EventRulesListResponse struct {
	Rules      []EventRule `json:"rules"`
	TotalCount int         `json:"total_count"`
	Page       int         `json:"page"`
	PageSize   int         `json:"page_size"`
}

// Event represents an event occurrence
type Event struct {
	EventID       int64           `json:"event_id" db:"event_id"`
	EventTypeID   int             `json:"event_type_id" db:"event_type_id"`
	DataID        *int64          `json:"data_id,omitempty" db:"data_id"`
	EventLevel    *string         `json:"event_level,omitempty" db:"event_level"` // manufacturer, system, automation
	EventRuleID   *string         `json:"event_rule_id,omitempty" db:"event_rule_id"`
	SpaceSlug     string          `json:"space_slug,omitempty" db:"space_slug"`
	EntityID      *string         `json:"entity_id,omitempty" db:"entity_id"`
	StateID       *int64          `json:"state_id,omitempty" db:"state_id"`
	ContextID     []byte          `json:"context_id_bin,omitempty" db:"context_id_bin"`
	TriggerID     *string         `json:"trigger_id,omitempty" db:"trigger_id"`
	TimeFiredTs   int64           `json:"time_fired_ts" db:"time_fired_ts"`
	CreatedAt     time.Time       `json:"created_at" db:"created_at"`

	// Joined fields
	EventType  string          `json:"event_type,omitempty" db:"event_type"`
	SharedData json.RawMessage `json:"shared_data,omitempty" db:"shared_data"`
}

// StatesMeta represents metadata about entity states
type StatesMeta struct {
	MetadataID int       `json:"metadata_id" db:"metadata_id"`
	EntityID   string    `json:"entity_id" db:"entity_id"`
	CreatedAt  time.Time `json:"created_at" db:"created_at"`
}

// StateAttributes represents shared state attributes (deduplicated by hash)
type StateAttributes struct {
	AttributesID  int             `json:"attributes_id" db:"attributes_id"`
	Hash          int64           `json:"hash" db:"hash"`
	SharedAttrs   json.RawMessage `json:"shared_attrs" db:"shared_attrs"`
	CreatedAt     time.Time       `json:"created_at" db:"created_at"`
}

// State represents an entity state with event linkage
type State struct {
	StateID       int64           `json:"state_id" db:"state_id"`
	MetadataID    int             `json:"metadata_id" db:"metadata_id"`
	State         string          `json:"state" db:"state"`
	AttributesID  *int            `json:"attributes_id,omitempty" db:"attributes_id"`
	EventID       *int64          `json:"event_id,omitempty" db:"event_id"`
	LastChangedTs int64           `json:"last_changed_ts" db:"last_changed_ts"`
	LastUpdatedTs int64           `json:"last_updated_ts" db:"last_updated_ts"`
	CreatedAt     time.Time       `json:"created_at" db:"created_at"`

	// Joined fields
	EntityID    string          `json:"entity_id,omitempty" db:"entity_id"`
	SharedAttrs json.RawMessage `json:"shared_attrs,omitempty" db:"shared_attrs"`
}

// StateChangeRequest represents a request to record a state change
type StateChangeRequest struct {
	EntityID       string                 `json:"entity_id"`
	SpaceSlug      string                 `json:"space_slug,omitempty"` // optional, will be resolved from headers if not provided
	NewState       string                 `json:"new_state"`
	OldState       string                 `json:"old_state,omitempty"`
	Attributes     map[string]interface{} `json:"attributes,omitempty"`
	EventType      string                 `json:"event_type,omitempty"` // defaults to "state_changed"
	TimeFiredTs    *int64                 `json:"time_fired_ts,omitempty"`
	ContextID      []byte                 `json:"context_id,omitempty"`
	TriggerID      *string                `json:"trigger_id,omitempty"`      // for future automations reference
}

// StateHistoryResponse represents historical state data for an entity
type StateHistoryResponse struct {
	EntityID    string                 `json:"entity_id"`
	State       string                 `json:"state"`
	Attributes  map[string]interface{} `json:"attributes,omitempty"`
	LastChanged time.Time              `json:"last_changed"`
	LastUpdated time.Time              `json:"last_updated"`
	EventID     *int64                 `json:"event_id,omitempty"`
}

// EventsListResponse represents paginated events
type EventsListResponse struct {
	Events     []Event `json:"events"`
	TotalCount int     `json:"total_count"`
	Page       int     `json:"page"`
	PageSize   int     `json:"page_size"`
}

// StateDetailResponse combines state with metadata and attributes
type StateDetailResponse struct {
	StateID         int64        `json:"state_id"`
	EntityID        string       `json:"entity_id"`
	State           string       `json:"state"`
	Attributes      map[string]interface{} `json:"attributes,omitempty"`
	LastChanged     time.Time    `json:"last_changed"`
	LastUpdated     time.Time    `json:"last_updated"`
	TriggeringEvent *EventDetail `json:"triggering_event,omitempty"`
}

// EventDetail represents a full event with its type and data
type EventDetail struct {
	EventID       int64                  `json:"event_id"`
	EventType     string                 `json:"event_type"`
	LevelEventID  *string                `json:"level_event_id,omitempty"`
	EventRuleID   *string                `json:"event_rule_id,omitempty"`
	SpaceSlug     string                 `json:"space_slug,omitempty"`
	EntityID      *string                `json:"entity_id,omitempty"`
	StateID       *int64                 `json:"state_id,omitempty"`
	TriggerID     *string                `json:"trigger_id,omitempty"`
	TimeFired     time.Time              `json:"time_fired"`
	EventData     map[string]interface{} `json:"event_data,omitempty"`
	ContextID     []byte                 `json:"context_id,omitempty"`

	// Joined fields
	EventRule *EventRule `json:"event_rule,omitempty"`
}

// TimestampsToTime converts timestamp fields to time.Time
func (s *State) LastChangedTime() time.Time {
	return time.UnixMilli(s.LastChangedTs)
}

// LastUpdatedTime converts the last_updated_ts to time.Time
func (s *State) LastUpdatedTime() time.Time {
	return time.UnixMilli(s.LastUpdatedTs)
}

// TimeFired converts the time_fired_ts to time.Time
func (e *Event) TimeFired() time.Time {
	return time.UnixMilli(e.TimeFiredTs)
}

// ParseAttributes parses the shared_attrs JSON into a map
func (s *State) ParseAttributes() (map[string]interface{}, error) {
	if s.SharedAttrs == nil {
		return nil, nil
	}
	var attrs map[string]interface{}
	if err := json.Unmarshal(s.SharedAttrs, &attrs); err != nil {
		return nil, err
	}
	return attrs, nil
}

// ParseEventData parses the shared_data JSON into a map
func (e *Event) ParseEventData() (map[string]interface{}, error) {
	if e.SharedData == nil {
		return nil, nil
	}
	var data map[string]interface{}
	if err := json.Unmarshal(e.SharedData, &data); err != nil {
		return nil, err
	}
	return data, nil
}

// SetSharedAttrs sets the shared_attrs from a map
func (s *StateAttributes) SetSharedAttrs(attrs map[string]interface{}) error {
	if attrs == nil {
		return nil
	}
	data, err := json.Marshal(attrs)
	if err != nil {
		return err
	}
	s.SharedAttrs = data
	return nil
}

// SetSharedData sets the shared_data from a map
func (e *EventData) SetSharedData(data map[string]interface{}) error {
	if data == nil {
		return nil
	}
	raw, err := json.Marshal(data)
	if err != nil {
		return err
	}
	e.SharedData = raw
	return nil
}

// MatchedEvent represents an event rule that matched evaluation
type MatchedEvent struct {
	EntityID    string  `json:"entity_id"`
	EntityType  string  `json:"entity_type"`
	RuleKey     string  `json:"rule_key"`
	EventType   string  `json:"event_type"`
	EventLevel  string  `json:"event_level"`
	Description string  `json:"description"`
	Value       float64 `json:"value"`
	Threshold   float64 `json:"threshold"`
	Operator    string  `json:"operator"`
	RuleSource  string  `json:"rule_source"` // "default" or "automation"
	Timestamp   int64   `json:"timestamp"`    // Unix timestamp in milliseconds
}
