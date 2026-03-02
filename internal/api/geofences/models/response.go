package models

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// GeofenceResponse represents a geofence in API responses
type GeofenceResponse struct {
	GeofenceID uuid.UUID       `json:"geofence_id"`
	Name       string          `json:"name"`
	TypeZone   string          `json:"type_zone"`
	Geometry   json.RawMessage `json:"geometry"`
	Features   []GeofenceFeature `json:"features,omitempty"`
	IsActive   bool            `json:"is_active"`
	SpaceID    *uuid.UUID      `json:"space_id,omitempty"`
	CreatedAt  time.Time       `json:"created_at"`
	UpdatedAt  time.Time       `json:"updated_at"`

	// Optional joined fields
	SpaceName *string `json:"space_name,omitempty"`
	SpaceSlug *string `json:"space_slug,omitempty"`
	SpaceLogo *string `json:"space_logo,omitempty"`
}

// GeofencesListResponse represents a paginated list of geofences
type GeofencesListResponse struct {
	Geofences  []GeofenceResponse `json:"geofences"`
	TotalCount int                `json:"total_count"`
	Page       int                `json:"page"`
	PageSize   int                `json:"page_size"`
}

// GeofencesByDeviceResponse represents geofences associated with a device
type GeofencesByDeviceResponse struct {
	DeviceID  string            `json:"device_id"`
	Geofences []GeofenceResponse `json:"geofences"`
	Count     int                `json:"count"`
}

// CreateGeofenceResponse represents response after creating a geofence
type CreateGeofenceResponse struct {
	Message   string           `json:"message"`
	Geofence  GeofenceResponse `json:"geofence"`
}

// UpdateGeofenceResponse represents response after updating a geofence
type UpdateGeofenceResponse struct {
	Message   string           `json:"message"`
	Geofence  GeofenceResponse `json:"geofence"`
}

// DeleteGeofenceResponse represents response after deleting a geofence
type DeleteGeofenceResponse struct {
	Message string `json:"message"`
}

// ErrorResponse represents an error response
type ErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message,omitempty"`
}