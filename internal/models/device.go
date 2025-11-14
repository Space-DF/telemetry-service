package models

import (
	"strconv"
	"strings"
	"time"

	dbmodels "github.com/Space-DF/telemetry-service/pkgs/db/models"
)

// DeviceLocationMessage represents the transformed device location message from RabbitMQ
type DeviceLocationMessage struct {
	DeviceEUI    string              `json:"device_eui"`
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
	Accuracy  string  `json:"accuracy,omitempty"` // e.g., "1_gateways", "triangulated"
}

func (m *DeviceLocationMessage) ToLocation() *dbmodels.DeviceLocation {
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

	// Parse gateway count from accuracy string (e.g., "1_gateways" -> 1)
	// Default to 1 if we have location data but no explicit gateway count
	var gatewayCount int32 // Default value
	gatewayCount = 1

	if strings.HasSuffix(m.Location.Accuracy, "_gateways") {
		gatewayStr := strings.TrimSuffix(m.Location.Accuracy, "_gateways")
		if count, err := strconv.Atoi(gatewayStr); err == nil {
			gatewayCount = int32(count) // #nosec G109,G115 -- gateway count is expected to be small
		}
	}

	return &dbmodels.DeviceLocation{
		Time:             ts,
		DeviceID:         m.DeviceID,
		OrganizationSlug: m.Organization,
		Latitude:         m.Location.Latitude,
		Longitude:        m.Location.Longitude,
		AccuracyGateways: gatewayCount,
		DeviceEui:        m.DeviceEUI,
	}
}
