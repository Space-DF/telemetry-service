package models

type GetDevicePropertiesRequest struct {
	DeviceID  string `query:"device_id" validate:"required"`
	SpaceSlug string `query:"space_slug" validate:"required"`
}

type DevicePropertiesResponse map[string]interface{}
