package models

// TelemetryPayload represents the entity-centric telemetry emitted by transformer-service.
type TelemetryPayload struct {
	Organization string            `json:"organization"`
	DeviceEUI    string            `json:"device_eui"`
	DeviceID     string            `json:"device_id,omitempty"`
	SpaceSlug    string            `json:"space_slug,omitempty"`
	DeviceInfo   TelemetryDevice   `json:"device_info"`
	Entities     []TelemetryEntity `json:"entities"`
	Timestamp    string            `json:"timestamp"`
	Source       string            `json:"source"`
	Metadata     map[string]any    `json:"metadata,omitempty"`
}

// TelemetryDevice holds basic device metadata.
type TelemetryDevice struct {
	Identifiers  []string `json:"identifiers"`
	Name         string   `json:"name"`
	Manufacturer string   `json:"manufacturer"`
	Model        string   `json:"model"`
	ModelID      string   `json:"model_id"`
}

// TelemetryEntity describes a single entity in telemetry payloads.
type TelemetryEntity struct {
	UniqueID    string         `json:"unique_id"`
	EntityID    string         `json:"entity_id"`
	EntityType  string         `json:"entity_type"`
	DeviceClass string         `json:"device_class,omitempty"`
	Name        string         `json:"name"`
	State       any            `json:"state"`
	DisplayType []string       `json:"display_type,omitempty"`
	Attributes  map[string]any `json:"attributes,omitempty"`
	UnitOfMeas  string         `json:"unit_of_measurement,omitempty"`
	Icon        string         `json:"icon,omitempty"`
	Timestamp   string         `json:"timestamp"`
	StateID     *string        `json:"state_id,omitempty"`
}
