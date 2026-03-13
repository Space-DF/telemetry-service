package models

// EntitiesRequest represents query parameters for entities list
type EntitiesRequest struct {
	SpaceSlug    string   `query:"space_slug" validate:"required"`
	Category     string   `query:"category"`
	DeviceID     string   `query:"device_id"`
	DisplayTypes []string `query:"display_type"`
	Search       string   `query:"search"`
}
