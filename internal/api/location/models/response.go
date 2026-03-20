package models

import "time"

// LocationResponse represents a single location
type LocationResponse struct {
	Timestamp time.Time `json:"timestamp" example:"2024-01-15T10:30:00Z"`
	Latitude  float64   `json:"latitude" example:"37.7749"`
	Longitude float64   `json:"longitude" example:"-122.4194"`
	DeviceID  string    `json:"device_id" example:"device-123"`
}
