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

// EventRule represents a rule that can trigger events
type EventRule struct {
	EventRuleID string `json:"event_rule_id" db:"event_rule_id"`
	RuleKey     string `json:"rule_key" db:"rule_key"`
	// Definition  *string   `json:"definition,omitempty" db:"definition"`
	Definition  json.RawMessage `json:"definition" db:"definition"`
	IsActive    *bool           `json:"is_active,omitempty" db:"is_active"`
	RepeatAble  *bool           `json:"repeat_able,omitempty" db:"repeat_able"`
	CooldownSec *int            `json:"cooldown_sec,omitempty" db:"cooldown_sec"`
	Description *string         `json:"description,omitempty" db:"description"`
	CreatedAt   time.Time       `json:"created_at" db:"created_at"`
}

// EventRuleRequest represents a request to create or update an event rule
type EventRuleRequest struct {
	RuleKey     *string `json:"rule_key" validate:"required"`
	Definition  *string `json:"definition"`
	IsActive    *bool   `json:"is_active"`
	RepeatAble  *bool   `json:"repeat_able"`
	CooldownSec *int    `json:"cooldown_sec"`
	Description *string `json:"description"`
}

// EventRuleResponse represents an event rule response
type EventRuleResponse struct {
	EventRuleID string    `json:"event_rule_id"`
	RuleKey     string    `json:"rule_key"`
	Definition  *string   `json:"definition,omitempty"`
	IsActive    bool      `json:"is_active"`
	RepeatAble  bool      `json:"repeat_able"`
	CooldownSec int       `json:"cooldown_sec"`
	Description *string   `json:"description,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
}

// Event represents an event occurrence
type Event struct {
	EventID      int64   `json:"event_id" db:"event_id"`
	EventTypeID  int     `json:"event_type_id" db:"event_type_id"`
	EventLevel   *string `json:"event_level,omitempty" db:"event_level"`     // manufacturer, system, automation
	EventRuleID  *string `json:"event_rule_id,omitempty" db:"event_rule_id"` // Rule that triggered this event
	AutomationID *string `json:"automation_id,omitempty" db:"automation_id"` // Automation that triggered this event
	GeofenceID   *string `json:"geofence_id,omitempty" db:"geofence_id"`     // Geofence that triggered this event
	SpaceSlug    string  `json:"space_slug,omitempty" db:"space_slug"`
	DeviceID     string  `json:"device_id,omitempty" db:"device_id"`
	EntityID     *string `json:"entity_id,omitempty" db:"entity_id"`
	StateID      *int64  `json:"state_id,omitempty" db:"state_id"`
	Title        string  `json:"title,omitempty" db:"title"`
	TimeFiredTs  int64   `json:"time_fired_ts" db:"time_fired_ts"`

	// Joined fields for internal use
	EventType          string `json:"event_type,omitempty" db:"event_type"`
	AutomationName     string `json:"automation_name,omitempty" db:"automation_name"`
	AutomationDeviceID string `json:"automation_device_id,omitempty" db:"automation_device_id"`
	GeofenceName       string `json:"geofence_name,omitempty" db:"geofence_name"`
	GeofenceTypeZone   string `json:"geofence_type_zone,omitempty" db:"geofence_type_zone"`

	Location *Location `json:"location,omitempty"`
}

// Location represents a geographic location
type Location struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

// StatesMeta represents metadata about entity states
type StatesMeta struct {
	MetadataID int       `json:"metadata_id" db:"metadata_id"`
	EntityID   string    `json:"entity_id" db:"entity_id"`
	CreatedAt  time.Time `json:"created_at" db:"created_at"`
}

// StateAttributes represents shared state attributes (deduplicated by hash)
type StateAttributes struct {
	AttributesID int             `json:"attributes_id" db:"attributes_id"`
	Hash         int64           `json:"hash" db:"hash"`
	SharedAttrs  json.RawMessage `json:"shared_attrs" db:"shared_attrs"`
	CreatedAt    time.Time       `json:"created_at" db:"created_at"`
}

// State represents an entity state with event linkage
type State struct {
	StateID       int64     `json:"state_id" db:"state_id"`
	MetadataID    int       `json:"metadata_id" db:"metadata_id"`
	State         string    `json:"state" db:"state"`
	AttributesID  *int      `json:"attributes_id,omitempty" db:"attributes_id"`
	EventID       *int64    `json:"event_id,omitempty" db:"event_id"`
	LastChangedTs int64     `json:"last_changed_ts" db:"last_changed_ts"`
	LastUpdatedTs int64     `json:"last_updated_ts" db:"last_updated_ts"`
	CreatedAt     time.Time `json:"created_at" db:"created_at"`

	// Joined fields
	EntityID    string          `json:"entity_id,omitempty" db:"entity_id"`
	SharedAttrs json.RawMessage `json:"shared_attrs,omitempty" db:"shared_attrs"`
}

// StateChangeRequest represents a request to record a state change
type StateChangeRequest struct {
	EntityID    string                 `json:"entity_id"`
	SpaceSlug   string                 `json:"space_slug,omitempty"` // optional, will be resolved from headers if not provided
	NewState    string                 `json:"new_state"`
	OldState    string                 `json:"old_state,omitempty"`
	Attributes  map[string]interface{} `json:"attributes,omitempty"`
	EventType   string                 `json:"event_type,omitempty"` // defaults to "state_changed"
	TimeFiredTs *int64                 `json:"time_fired_ts,omitempty"`
	ContextID   []byte                 `json:"context_id,omitempty"`
	TriggerID   *string                `json:"trigger_id,omitempty"` // for future automations reference
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
	StateID         int64                  `json:"state_id"`
	EntityID        string                 `json:"entity_id"`
	State           string                 `json:"state"`
	Attributes      map[string]interface{} `json:"attributes,omitempty"`
	LastChanged     time.Time              `json:"last_changed"`
	LastUpdated     time.Time              `json:"last_updated"`
	TriggeringEvent *EventDetail           `json:"triggering_event,omitempty"`
}

// EventDetail represents a full event with its type and data
type EventDetail struct {
	EventID      int64                  `json:"event_id"`
	EventType    string                 `json:"event_type"`
	LevelEventID *string                `json:"level_event_id,omitempty"`
	EventRuleID  *string                `json:"event_rule_id,omitempty"` // Rule that triggered this event
	AutomationID *string                `json:"automation_id,omitempty"` // Automation that triggered this event
	GeofenceID   *string                `json:"geofence_id,omitempty"`   // Geofence that triggered this event
	SpaceSlug    string                 `json:"space_slug,omitempty"`
	EntityID     *string                `json:"entity_id,omitempty"`
	StateID      *int64                 `json:"state_id,omitempty"`
	TriggerID    *string                `json:"trigger_id,omitempty"`
	TimeFired    time.Time              `json:"time_fired"`
	EventData    map[string]interface{} `json:"event_data,omitempty"`
	ContextID    []byte                 `json:"context_id,omitempty"`
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

// MatchedEvent represents an event rule that matched evaluation
type MatchedEvent struct {
	DeviceID     string    `json:"device_id"`
	EntityType   string    `json:"entity_type"`
	RuleKey      string    `json:"rule_key"`
	EventType    string    `json:"event_type"`
	EventLevel   string    `json:"event_level"`
	Title        string    `json:"title"`
	Description  string    `json:"description"`
	Value        float64   `json:"value"`
	Threshold    float64   `json:"threshold"`
	Operator     string    `json:"operator"`
	Timestamp    int64     `json:"timestamp"`               // Unix timestamp in milliseconds
	EventRuleID  *string   `json:"event_rule_id,omitempty"` // Rule that triggered this event
	AutomationID *string   `json:"automation_id,omitempty"` // Automation that triggered this event
	GeofenceID   *string   `json:"geofence_id,omitempty"`   // Geofence that triggered this event
	StateID      *string   `json:"state_id,omitempty"`
	Location     *Location `json:"location,omitempty"`
}
