package models

// EntitiesRequest represents query parameters for entities list
type EntitiesRequest struct {
	SpaceSlug    string   `query:"space_slug" validate:"required"`
	Category     string   `query:"category"`
	DeviceID     string   `query:"device_id"`
	DisplayTypes []string `query:"display_type"`
	Search       string   `query:"search"`
	Page         int      `query:"page"`
	PageSize     int      `query:"page_size"`
}

// SetDefaults sets default values for pagination
func (r *EntitiesRequest) SetDefaults() {
	if r.Page < 1 {
		r.Page = 1
	}
	if r.PageSize < 1 {
		r.PageSize = 100
	}
}
