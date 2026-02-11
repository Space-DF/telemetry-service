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

// EventRulesRequest represents query parameters for event rules list
type EventRulesRequest struct {
	Page     int    `query:"page"`
	PageSize int    `query:"page_size"`
	DeviceID string `query:"device_id"`
}

// SetDefaults sets default values
func (r *EventRulesRequest) SetDefaults() {
	if r.Page < 1 {
		r.Page = 1
	}
	if r.PageSize < 1 || r.PageSize > 100 {
		r.PageSize = 20
	}
}
