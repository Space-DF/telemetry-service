package models

import "time"

// LocationHistoryResponse represents the response for location history
type LocationHistoryResponse struct {
	DeviceID         string              `json:"device_id"`
	OrganizationSlug string              `json:"organization_slug_id"`
	Count            int                 `json:"count"`
	Locations        []LocationResponse  `json:"locations"`
	QueryParams      QueryParamsResponse `json:"query_params"`
}

// LocationResponse represents a single location
type LocationResponse struct {
	Timestamp time.Time `json:"timestamp"`
	Latitude  float64   `json:"latitude"`
	Longitude float64   `json:"longitude"`
	Accuracy  float64   `json:"accuracy"`
	DeviceID  string    `json:"device_id"`
}

// QueryParamsResponse shows the actual query parameters used
type QueryParamsResponse struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
	Limit int       `json:"limit"`
}
