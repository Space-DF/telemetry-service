package models

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// EventRuleInfo represents basic event rule information
type EventRuleInfo struct {
	EventRuleID string          `json:"event_rule_id"`
	RuleKey     string          `json:"rule_key"`
	Definition  json.RawMessage `json:"definition,omitempty" swaggertype:"object"`
	IsActive    bool            `json:"is_active"`
	CreatedAt   time.Time       `json:"created_at"`
}

// GeofenceResponse represents a geofence in API responses
type GeofenceResponse struct {
	GeofenceID uuid.UUID         `json:"id"`
	Name       string            `json:"name"`
	TypeZone   string            `json:"type_zone"`
	Features   []json.RawMessage `json:"features" swaggertype:"array,object"`
	Color      string            `json:"color"`
	EventRule  *EventRuleInfo    `json:"event_rule,omitempty"`
	IsActive   bool              `json:"is_active"`
	SpaceID    *uuid.UUID        `json:"space_id,omitempty"`
	CreatedAt  time.Time         `json:"created_at"`
	UpdatedAt  time.Time         `json:"updated_at"`
}

// GeofenceDetailResponse represents a detailed geofence with additional info
type GeofenceDetailResponse struct {
	GeofenceID  uuid.UUID       `json:"id"`
	Name        string          `json:"name"`
	TypeZone    string          `json:"type_zone"`
	Geometry    json.RawMessage `json:"geometry" swaggertype:"object"`
	EventRuleID *uuid.UUID      `json:"event_rule_id,omitempty"`
	IsActive    bool            `json:"is_active"`
	SpaceID     *uuid.UUID      `json:"space_id,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`

	// Space details
	Space     *SpaceItem  `json:"space,omitempty"`
	DeviceIDs []uuid.UUID `json:"device_ids,omitempty"` // Devices associated with this geofence
}

// SpaceResponse represents a space in API responses
type SpaceResponse struct {
	SpaceID      uuid.UUID  `json:"space_id"`
	Name         string     `json:"name"`
	Logo         *string    `json:"logo,omitempty"`
	SpaceSlug    string     `json:"space_slug"`
	IsActive     bool       `json:"is_active"`
	TotalDevices *int       `json:"total_devices,omitempty"`
	Description  *string    `json:"description,omitempty"`
	CreatedBy    *uuid.UUID `json:"created_by,omitempty"`
	CreatedAt    time.Time  `json:"created_at"`
}

// SpacesListResponse represents a paginated list of spaces
type SpacesListResponse struct {
	Spaces     []SpaceResponse `json:"spaces"`
	TotalCount int             `json:"total_count"`
	Page       int             `json:"page"`
	PageSize   int             `json:"page_size"`
}

// SpaceItem represents a minimal space info
type SpaceItem struct {
	SpaceID   uuid.UUID `json:"space_id"`
	Name      string    `json:"name"`
	SpaceSlug string    `json:"space_slug"`
	Logo      *string   `json:"logo,omitempty"`
}

// GeofencesBySpaceResponse represents geofences associated with a space
type GeofencesBySpaceResponse struct {
	SpaceID string             `json:"space_id"`
	Results []GeofenceResponse `json:"results"`
	Count   int                `json:"count"`
}

// PointInGeofenceResponse represents the result of a point-in-geofence check
type PointInGeofenceResponse struct {
	GeofenceID uuid.UUID `json:"id"`
	Name       string    `json:"name"`
	IsInside   bool      `json:"is_inside"`
}

// PointInAnyGeofenceResponse represents the result of checking if a point is in any geofence of a space
type PointInAnyGeofenceResponse struct {
	SpaceID   uuid.UUID                 `json:"space_id"`
	IsInside  bool                      `json:"is_inside"`
	Geofences []PointInGeofenceResponse `json:"geofences"`
	Count     int                       `json:"count"`
}

// CreateGeofenceResponse represents response after creating a geofence
type CreateGeofenceResponse struct {
	Message  string           `json:"message"`
	Geofence GeofenceResponse `json:"geofence"`
}

// UpdateGeofenceResponse represents response after updating a geofence
type UpdateGeofenceResponse struct {
	Message  string           `json:"message"`
	Geofence GeofenceResponse `json:"geofence"`
}

// DeleteGeofenceResponse represents response after deleting a geofence
type DeleteGeofenceResponse struct {
	Message string `json:"message"`
}

// CreateSpaceResponse represents response after creating a space
type CreateSpaceResponse struct {
	Message string        `json:"message"`
	Space   SpaceResponse `json:"space"`
}

// UpdateSpaceResponse represents response after updating a space
type UpdateSpaceResponse struct {
	Message string        `json:"message"`
	Space   SpaceResponse `json:"space"`
}

// DeleteSpaceResponse represents response after deleting a space
type DeleteSpaceResponse struct {
	Message string `json:"message"`
}

// ErrorResponse represents an error response
type ErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message,omitempty"`
}

// ResultResponse represents a result response
type ResultResponse struct {
	Result string `json:"result"`
}

// TestGeofenceResponse represents the result of testing a geofence configuration against all devices
type TestGeofenceResponse struct {
	Name      string                 `json:"name,omitempty"` // Optional name provided in request
	TypeZone  string                 `json:"type_zone"`      // Zone type from request
	Devices   []DeviceGeofenceResult `json:"devices"`        // Test results per device
	Total     int                    `json:"total"`          // Total devices tested
	Triggered int                    `json:"triggered"`      // Number of devices that triggered
}

// DeviceGeofenceResult represents the geofence test result for a single device
type DeviceGeofenceResult struct {
	DeviceID   string  `json:"device_id"`
	Latitude   float64 `json:"latitude"`
	Longitude  float64 `json:"longitude"`
	IsInside   bool    `json:"is_inside"`
	DistanceKm float64 `json:"distance_km"`
	Triggered  bool    `json:"triggered"`
	Reason     string  `json:"reason"`
}
