package models

import "time"

// LocationHistoryResponse represents the response for location history
type LocationHistoryResponse struct {
	Count     int                `json:"count" example:"100"`
	Locations []LocationResponse `json:"locations"`
}

// LocationResponse represents a single location
type LocationResponse struct {
	Timestamp time.Time `json:"timestamp" example:"2024-01-15T10:30:00Z"`
	Latitude  float64   `json:"latitude" example:"37.7749"`
	Longitude float64   `json:"longitude" example:"-122.4194"`
	DeviceID  string    `json:"device_id" example:"device-123"`
}

// QueryParamsResponse shows the actual query parameters used
type QueryParamsResponse struct {
	Start time.Time `json:"start" example:"2024-01-01T00:00:00Z"`
	End   time.Time `json:"end" example:"2024-01-15T23:59:59Z"`
	Limit int       `json:"limit" example:"100"`
}
