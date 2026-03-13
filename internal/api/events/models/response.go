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
	EventID    int64                  `json:"event_id"`
	EventType  string                 `json:"event_type"`
	EventLevel string                 `json:"event_level,omitempty"`
	SpaceSlug  string                 `json:"space_slug,omitempty"`
	EntityID   string                 `json:"entity_id,omitempty"`
	TimeFired  time.Time              `json:"time_fired"`
	EventData  map[string]interface{} `json:"event_data,omitempty"`
}

// DeleteEventRuleResponse represents response after deleting an event rule
type DeleteEventRuleResponse struct {
	Message string `json:"message"`
}
