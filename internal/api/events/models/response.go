package models

import "time"

// EventsByDeviceResponse represents response for device events
type EventsByDeviceResponse struct {
	DeviceID string      `json:"device_id"`
	Events   []EventItem `json:"events"`
	Count    int         `json:"count"`
}

// EventItem represents a single event in API response
type EventItem struct {
	EventID     int64                  `json:"event_id"`
	EventType   string                 `json:"event_type"`
	EventLevel  string                 `json:"event_level,omitempty"`
	SpaceSlug   string                 `json:"space_slug,omitempty"`
	EntityID    string                 `json:"entity_id,omitempty"`
	TimeFired   time.Time              `json:"time_fired"`
	EventData   map[string]interface{} `json:"event_data,omitempty"`
}

// EventRulesResponse represents paginated event rules response
type EventRulesResponse struct {
	Rules      []EventRuleItem `json:"rules"`
	TotalCount int             `json:"total_count"`
	Page       int             `json:"page"`
	PageSize   int             `json:"page_size"`
}

// EventRuleItem represents a single event rule in API response
type EventRuleItem struct {
	EventRuleID string    `json:"event_rule_id"`
	DeviceID    *string   `json:"device_id,omitempty"`
	SpaceID     *string   `json:"space_id,omitempty"`
	GeofenceID  *string   `json:"geofence_id,omitempty"`
	RuleKey     string    `json:"rule_key,omitempty"`
	Definition  *string   `json:"definition,omitempty"`
	IsActive    bool      `json:"is_active"`
	RepeatAble  bool      `json:"repeat_able"`
	CooldownSec *int      `json:"cooldown_sec,omitempty"`
	Description *string   `json:"description,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// DeleteEventRuleResponse represents response after deleting an event rule
type DeleteEventRuleResponse struct {
	Message string `json:"message"`
}
