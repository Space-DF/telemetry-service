package models

import (
	"encoding/json"

	"github.com/google/uuid"
)

// GeofenceFeature represents a single polygon feature in a geofence
type GeofenceFeature struct {
	ID       uuid.UUID       `json:"id"`
	Type     string          `json:"type"` // "Feature"
	Geometry json.RawMessage `json:"geometry"`
}

type FeatureProperties struct {
	Mode string `json:"mode"`
}

// CreateGeofenceRequest represents a request to create a new geofence
type CreateGeofenceRequest struct {
	Name       string            `json:"name" validate:"required,min=1,max=100"`
	Type       string            `json:"type_zone" validate:"required,oneof=safe danger normal"` // "type_zone" from frontend
	Geometry   []GeofenceFeature `json:"geometry" validate:"required"`                           // Multiple polygons
	SpaceID    *uuid.UUID        `json:"space_id,omitempty" validate:"omitempty,uuid"`
	IsActive   *bool             `json:"is_active,omitempty"`
	Definition json.RawMessage   `json:"definition,omitempty"`
}

// UpdateGeofenceRequest represents a request to update a geofence
type UpdateGeofenceRequest struct {
	Name       *string           `json:"name,omitempty" validate:"omitempty,min=1,max=100"`
	Type       *string           `json:"type_zone,omitempty" validate:"omitempty,oneof=safe danger normal"`
	Geometry   []GeofenceFeature `json:"geometry,omitempty"`
	SpaceID    *uuid.UUID        `json:"space_id,omitempty" validate:"omitempty,uuid"`
	IsActive   *bool             `json:"is_active,omitempty"`
	Definition json.RawMessage   `json:"definition,omitempty"` // Event rule definition as JSON object
}

// ListGeofencesRequest represents query parameters for listing geofences
type ListGeofencesRequest struct {
	SpaceID  *uuid.UUID `query:"space_id" validate:"omitempty,uuid"`
	IsActive *bool      `query:"is_active"`
	Page     int        `query:"page" validate:"omitempty,min=1"`
	PageSize int        `query:"page_size" validate:"omitempty,min=1,max=100"`
}

// ListSpacesRequest represents query parameters for listing spaces
type ListSpacesRequest struct {
	IsActive  *bool `query:"is_active"`
	IsDefault *bool `query:"is_default"`
	Page      int   `query:"page" validate:"omitempty,min=1"`
	PageSize  int   `query:"page_size" validate:"omitempty,min=1,max=100"`
}

// CheckPointInGeofenceRequest represents a request to check if a point is within a geofence
type CheckPointInGeofenceRequest struct {
	GeofenceID uuid.UUID `json:"geofence_id" validate:"required,uuid"`
	Latitude   float64   `json:"latitude" validate:"required,min=-90,max=90"`
	Longitude  float64   `json:"longitude" validate:"required,min=-180,max=180"`
}

// CheckPointInAnyGeofenceRequest represents a request to check if a point is within any geofence in a space
type CheckPointInAnyGeofenceRequest struct {
	SpaceID   uuid.UUID `json:"space_id" validate:"required,uuid"`
	Latitude  float64   `json:"latitude" validate:"required,min=-90,max=90"`
	Longitude float64   `json:"longitude" validate:"required,min=-180,max=180"`
}

// GetGeofencesByDeviceRequest represents a request to get geofences associated with a device
type GetGeofencesByDeviceRequest struct {
	DeviceID uuid.UUID `param:"device_id" validate:"required,uuid"`
	IsActive *bool     `query:"is_active"`
}
