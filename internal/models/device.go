package models

import (
	"time"

	dbmodels "github.com/Space-DF/telemetry-service/pkgs/db/models"
	"github.com/aarondl/opt/omit"
)

// DeviceLocationMessage represents the transformed device location message from RabbitMQ
type DeviceLocationMessage struct {
	DeviceID     string              `json:"device_id"`
	Location     LocationCoordinates `json:"location"`
	Timestamp    string              `json:"timestamp"`
	Organization string              `json:"organization"`
	Source       string              `json:"source"`
	Metadata     map[string]any      `json:"metadata"`
}

// LocationCoordinates represents geographic coordinates with accuracy
type LocationCoordinates struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	Accuracy  float64 `json:"accuracy"`
	Direction *string `json:"direction,omitempty"`
}

func (m *DeviceLocationMessage) ToLocationSetter() *dbmodels.DeviceLocationSetter {
	// Skip if device_id is unknown or empty - we can't store locations without proper device identification
	if m.DeviceID == "" || m.DeviceID == "unknown" {
		return nil
	}

	// Skip if we don't have valid coordinates
	if m.Location.Latitude == 0 && m.Location.Longitude == 0 {
		return nil
	}

	// Parse timestamp
	ts, err := time.Parse(time.RFC3339, m.Timestamp)
	if err != nil {
		ts = time.Now()
	}

	return &dbmodels.DeviceLocationSetter{
		Time:             omit.From(ts),
		DeviceID:         omit.From(m.DeviceID),
		OrganizationSlug: omit.From(m.Organization),
		Latitude:         omit.From(m.Location.Latitude),
		Longitude:        omit.From(m.Location.Longitude),
		Accuracy:         omit.From(m.Location.Accuracy),
	}
}
