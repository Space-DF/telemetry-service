package models

// EntitiesRequest represents query parameters for entities list
type EntitiesRequest struct {
	SpaceSlug    string   `query:"space_slug" validate:"required"`
	Category     string   `query:"category"`
	DeviceID     string   `query:"device_id"`
	DevEUI       string   `query:"dev_eui"`
	DisplayTypes []string `query:"display_type"`
	Search       string   `query:"search"`
}

// UpdateEntityRequest represents partial updates for an entity.
type UpdateEntityRequest struct {
	EntityIDs []string `json:"entity_ids"`
	IsEnabled *bool    `json:"is_enabled"`
}
