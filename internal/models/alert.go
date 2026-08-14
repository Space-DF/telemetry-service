package models

import (
	"time"
)

type Alert struct {
	ID           int64      `json:"id"`
	Type         string     `json:"type"`
	Title        string     `json:"title,omitempty"`
	Level        *string    `json:"level,omitempty"`
	Unit         string     `json:"unit,omitempty"`
	WaterDepth   *string    `json:"water_depth,omitempty"`
	Organization string     `json:"organization,omitempty"`
	SpaceSlug    string     `json:"space_slug,omitempty"`
	IsPublic     bool       `json:"is_public,omitempty"`
	DeviceID     string     `json:"device_id,omitempty"`
	EntityID     *string    `json:"entity_id,omitempty"`
	Message      string     `json:"message,omitempty"`
	Location     *Location  `json:"location,omitempty"`
	ReportedAt   time.Time  `json:"reported_at,omitempty"`
	Threshold    *Threshold `json:"threshold,omitempty"`
}

type Threshold struct {
	Caution  float64 `json:"caution,omitempty"`
	Critical float64 `json:"critical,omitempty"`
	Warning  float64 `json:"warning,omitempty"`
}
