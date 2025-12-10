package models

// DeviceLocationMessage represents the transformed device location message from RabbitMQ
type DeviceLocationMessage struct {
	DeviceID     string              `json:"device_id"`
	Location     LocationCoordinates `json:"location"`
	Timestamp    string              `json:"timestamp"`
	Space        string              `json:"space_slug"`
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

// ToTelemetryPayload converts DeviceLocationMessage to TelemetryPayload for entity_states storage
func (m *DeviceLocationMessage) ToTelemetryPayload() *TelemetryPayload {
	// Skip if device_id is unknown or empty
	if m.DeviceID == "" || m.DeviceID == "unknown" {
		return nil
	}

	// Skip if we don't have valid coordinates
	if m.Location.Latitude == 0 && m.Location.Longitude == 0 {
		return nil
	}

	return &TelemetryPayload{
		DeviceID:     m.DeviceID,
		Organization: m.Organization,
		SpaceSlug:    m.Space,
		Entities:     []TelemetryEntity{},
		Timestamp:    m.Timestamp,
		Source:       m.Source,
		Metadata:     m.Metadata,
	}
}
