package models

// GetDevicePropertiesRequest represents query parameters for device properties
type GetDevicePropertiesRequest struct {
	DeviceID  string `query:"device_id" validate:"required"`
	SpaceSlug string `query:"space_slug" validate:"required"`
}
