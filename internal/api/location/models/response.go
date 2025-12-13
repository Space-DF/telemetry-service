package models

import "time"

// LocationHistoryResponse represents the response for location history
type LocationHistoryResponse struct {
	Count     int                `json:"count"`
	Locations []LocationResponse `json:"locations"`
}

// LocationResponse represents a single location
type LocationResponse struct {
	Timestamp time.Time `json:"timestamp"`
	Latitude  float64   `json:"latitude"`
	Longitude float64   `json:"longitude"`
	DeviceID  string    `json:"device_id"`
}

// QueryParamsResponse shows the actual query parameters used
type QueryParamsResponse struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
	Limit int       `json:"limit"`
}
