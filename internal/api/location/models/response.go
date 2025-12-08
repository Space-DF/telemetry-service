package models

import "time"

// LocationHistoryResponse represents the response for location history
type LocationHistoryResponse struct {
	DeviceID    string              `json:"device_id"`
	SpaceSlug   string              `json:"space_slug_id"`
	Count       int                 `json:"count"`
	Locations   []LocationResponse  `json:"locations"`
	QueryParams QueryParamsResponse `json:"query_params"`
}

// LocationResponse represents a single location
type LocationResponse struct {
	Timestamp time.Time `json:"timestamp"`
	Latitude  float64   `json:"latitude"`
	Longitude float64   `json:"longitude"`
	Accuracy  float64   `json:"accuracy"`
	DeviceID  string    `json:"device_id"`
	// Attributes contains the shared attributes recorded for the device
	// at (or before) this timestamp, if any. It is returned as a
	// map[string]interface{} so JSON will marshal arbitrary attribute
	// objects produced by devices/transformer.
	Attributes map[string]interface{} `json:"attributes,omitempty"`
}

// QueryParamsResponse shows the actual query parameters used
type QueryParamsResponse struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
	Limit int       `json:"limit"`
}

// LastLocationResponse represents the response for the last location
type LastLocationResponse struct {
	DeviceID  string    `json:"device_id"`
	SpaceSlug string    `json:"space_slug"`
	Timestamp time.Time `json:"timestamp"`
	Latitude  float64   `json:"latitude"`
	Longitude float64   `json:"longitude"`
	Accuracy  float64   `json:"accuracy"`
}
