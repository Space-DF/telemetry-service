package models

// GetDevicePropertiesBatchRequest represents a batch device properties request.
type GetDevicePropertiesBatchRequest struct {
	DeviceIDs []string          `json:"device_ids"`
	EndDates  map[string]string `json:"end_dates,omitempty"`
}
