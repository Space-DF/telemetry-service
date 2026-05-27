package models

import "github.com/google/uuid"

// EntitiesRequest represents query parameters for entities list
type EntitiesRequest struct {
	Category     string   `query:"category"`
	DeviceID     string   `query:"device_id"`
	DevEUI       string   `query:"dev_eui"`
	DisplayTypes []string `query:"display_type"`
	Search       string   `query:"search"`
}

// UpdateEntityRequest represents partial updates for an entity.
type UpdateEntityRequest struct {
	All               *bool       `json:"all"`
	IsEnabled         *bool       `json:"is_enabled"`
	ExcludedEntityIDs []uuid.UUID `json:"excluded_entity_ids"`
	VisibleEntityIDs  []uuid.UUID `json:"visible_entity_ids"`
	HiddenEntityIDs   []uuid.UUID `json:"hidden_entity_ids"`
}
