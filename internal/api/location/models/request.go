package models

import (
	"fmt"
	"time"
)

// LocationHistoryRequest represents query parameters for location history
type LocationHistoryRequest struct {
	DeviceID  string    `query:"device_id" validate:"required"`
	SpaceSlug string    `query:"space_slug" validate:"required"`
	Start     time.Time `query:"start"`
	End       time.Time `query:"end"`
	Limit     int       `query:"limit"`
}

func (r LocationHistoryRequest) Validate() (*LocationHistoryRequest, error) {
	if r.DeviceID == "" {
		return nil, fmt.Errorf("device_id is required")
	}

	if r.SpaceSlug == "" {
		return nil, fmt.Errorf("space_slug is required")
	}
	if r.Start.IsZero() {
		return nil, fmt.Errorf("start time is required")
	}
	if r.End.IsZero() {
		r.End = time.Now()
	}
	if r.End.Before(r.Start) {
		return nil, fmt.Errorf("end time must be after start time")
	}

	return &r, nil
}

// LastLocationRequest represents query parameters for getting the last location
type LastLocationRequest struct {
	DeviceID  string `query:"device_id" validate:"required"`
	SpaceSlug string `query:"space_slug" validate:"required"`
}

func (r LastLocationRequest) Validate() (*LastLocationRequest, error) {
	if r.DeviceID == "" {
		return nil, fmt.Errorf("device_id is required")
	}

	if r.SpaceSlug == "" {
		return nil, fmt.Errorf("space_slug is required")
	}

	return &r, nil
}
