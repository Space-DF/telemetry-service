package models

import (
	"time"
)

// AutomationSummary represents summary info about an automation for event responses
type AutomationSummary struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// GeofenceSummary represents summary info about a geofence for event responses
type GeofenceSummary struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	TypeZone string `json:"type_zone,omitempty"`
}

// LocationData represents geographic coordinates
type LocationData struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

// EventItem represents a single event in API response
type EventItem struct {
	EventID    int64              `json:"id"`
	EventType  string             `json:"event_type"`
	EventLevel string             `json:"event_level,omitempty"`
	Title      string             `json:"title,omitempty"`
	EntityID   string             `json:"entity_id,omitempty"`
	TimeFired  time.Time          `json:"time_fired"`
	Automation *AutomationSummary `json:"automation,omitempty"`
	Geofence   *GeofenceSummary   `json:"geofence,omitempty"`
	Location   *LocationData      `json:"location,omitempty"`
}

// DeleteEventRuleResponse represents response after deleting an event rule
type DeleteEventRuleResponse struct {
	Message string `json:"message"`
}
