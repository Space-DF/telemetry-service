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

// ListGeofencesRequest represents query parameters for listing geofences
type ListGeofencesRequest struct {
	SpaceID  *uuid.UUID `query:"space_id" validate:"omitempty,uuid"`
	IsActive *bool      `query:"is_active"`
	Page     int        `query:"page" validate:"omitempty,min=1"`
	PageSize int        `query:"page_size" validate:"omitempty,min=1,max=100"`
}


