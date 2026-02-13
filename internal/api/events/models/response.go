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
	EventRuleID   string     `json:"event_rule_id"`
	DeviceID      *string    `json:"device_id,omitempty"`
	RuleKey       string     `json:"rule_key,omitempty"`
	Operator      string     `json:"operator,omitempty"`
	Operand       string     `json:"operand"`
	IsActive      bool       `json:"is_active"`
	StartTime     *time.Time `json:"start_time,omitempty"`
	EndTime       *time.Time `json:"end_time,omitempty"`
	AllowNewEvent bool       `json:"allow_new_event"`
	CreatedAt     time.Time  `json:"created_at"`
	UpdatedAt     time.Time  `json:"updated_at"`
}

// DeleteEventRuleResponse represents response after deleting an event rule
type DeleteEventRuleResponse struct {
	Message string `json:"message"`
}
