package models

// EventsByDeviceRequest represents query parameters for device events
type EventsByDeviceRequest struct {
	DeviceID  string `param:"device_id" validate:"required"`
	Limit     int    `query:"limit"`
	StartTime *int64 `query:"start_time"`
	EndTime   *int64 `query:"end_time"`
}

// SetDefaults sets default values
func (r *EventsByDeviceRequest) SetDefaults() {
	if r.Limit < 1 {
		r.Limit = 100
	}
}
