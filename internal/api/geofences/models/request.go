package models

import (
	"encoding/json"

	"github.com/google/uuid"
)

// GeofenceFeature represents a single polygon feature in a geofence
type GeofenceFeature struct {
	ID         uuid.UUID       `json:"id"`
	Type       string          `json:"type"` // "Feature"
	Geometry   json.RawMessage `json:"geometry"`
}
type FeatureProperties struct {
		Mode  string `json:"mode"`
}

// CreateGeofenceRequest represents a request to create a new geofence
type CreateGeofenceRequest struct {
	Name      string                 `json:"name" validate:"required,min=1,max=100"`
	Type      string                 `json:"type" validate:"required,oneof=safe danger normal"` // "type" from frontend
	Features  []GeofenceFeature       `json:"features" validate:"required"` // Multiple polygons
	SpaceID   *uuid.UUID             `json:"space_id,omitempty" validate:"omitempty,uuid"`
	IsActive  *bool                  `json:"is_active,omitempty"`
}

// UpdateGeofenceRequest represents a request to update a geofence
type UpdateGeofenceRequest struct {
	Name      *string                `json:"name,omitempty" validate:"omitempty,min=1,max=100"`
	Type      *string                `json:"type,omitempty" validate:"omitempty,oneof=safe danger normal"`
	Features  []GeofenceFeature       `json:"features,omitempty"`
	SpaceID   *uuid.UUID             `json:"space_id,omitempty" validate:"omitempty,uuid"`
	IsActive  *bool                  `json:"is_active,omitempty"`
}

// CreateSpaceRequest represents a request to create a new space
type CreateSpaceRequest struct {
	Name         string     `json:"name" validate:"required,min=1,max=100"`
	Logo         *string    `json:"logo,omitempty" validate:"omitempty,url"`
	SpaceSlug    string     `json:"space_slug" validate:"required,min=1,max=100,alphanum"`
	IsActive     *bool      `json:"is_active,omitempty"`
	TotalDevices *int       `json:"total_devices,omitempty" validate:"omitempty,min=0"`
	Description  *string    `json:"description,omitempty"`
	CreatedBy    *uuid.UUID `json:"created_by,omitempty" validate:"omitempty,uuid"`
}

// UpdateSpaceRequest represents a request to update a space
type UpdateSpaceRequest struct {
	Name         *string `json:"name,omitempty" validate:"omitempty,min=1,max=100"`
	Logo         *string `json:"logo,omitempty" validate:"omitempty,url"`
	SpaceSlug    *string `json:"space_slug,omitempty" validate:"omitempty,min=1,max=100,alphanum"`
	IsActive     *bool   `json:"is_active,omitempty"`
	TotalDevices *int    `json:"total_devices,omitempty" validate:"omitempty,min=0"`
	Description  *string `json:"description,omitempty"`
}

// ListGeofencesRequest represents query parameters for listing geofences
type ListGeofencesRequest struct {
	SpaceID  *uuid.UUID `query:"space_id" validate:"omitempty,uuid"`
	IsActive *bool      `query:"is_active"`
	Type     *string    `query:"type" validate:"omitempty,oneof=safe danger normal"`
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


