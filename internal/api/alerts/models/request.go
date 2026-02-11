package models

import "time"

// AlertsRequest represents query parameters for alerts
type AlertsRequest struct {
	SpaceSlug          string     `query:"space_slug"`
	DeviceID           string     `query:"device_id" validate:"required"`
	Category           string     `query:"category" validate:"required"`
	StartDate          string     `query:"start_date"`
	EndDate            string     `query:"end_date"`
	Page               int        `query:"page"`
	PageSize           int        `query:"page_size"`
	CautionThreshold   float64    `query:"caution_threshold"`
	WarningThreshold   float64    `query:"warning_threshold"`
	CriticalThreshold  float64    `query:"critical_threshold"`
}

// Alert represents a single alert
type Alert struct {
	ID         string                 `json:"id"`
	Type       string                 `json:"type"`
	Level      string                 `json:"level"`
	Message    string                 `json:"message"`
	EntityID   string                 `json:"entity_id"`
	EntityName string                 `json:"entity_name"`
	DeviceID   string                 `json:"device_id"`
	SpaceSlug  string                 `json:"space_slug"`
	Location   *LocationInfo          `json:"location,omitempty"`
	WaterDepth float64                `json:"water_depth"`
	Unit       string                 `json:"unit"`
	Threshold  *ThresholdInfo         `json:"threshold"`
	ReportedAt time.Time              `json:"reported_at"`
	Attributes map[string]interface{} `json:"attributes,omitempty"`
}

// LocationInfo represents location information for an alert
type LocationInfo struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	Address   string  `json:"address,omitempty"`
}

// ThresholdInfo represents threshold levels
type ThresholdInfo struct {
	Warning  float64 `json:"warning"`
	Critical float64 `json:"critical"`
}
