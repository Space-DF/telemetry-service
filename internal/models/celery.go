package models

import (
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
)

// SpaceData represents the space data sent in Celery update_space task
type SpaceData struct {
	ID           uuid.UUID `json:"-"`
	Name         string    `json:"name"`
	Logo         *string   `json:"logo,omitempty"`
	SlugName     string    `json:"slug_name"`
	IsActive     bool      `json:"is_active"`
	IsDefault    bool      `json:"is_default"`
	TotalDevices int       `json:"total_devices"`
	Description  *string   `json:"description,omitempty"`
	CreatedBy    uuid.UUID `json:"-"`
}

// djangoUUID represents Django's UUID serialization format
type djangoUUID struct {
	Type  string `json:"__type__"`
	Value struct {
		Hex string `json:"hex"`
	} `json:"__value__"`
}

// UnmarshalJSON implements custom JSON unmarshaling for SpaceData
// to handle UUID and datetime fields that come in Django's special format
func (s *SpaceData) UnmarshalJSON(data []byte) error {
	// Parse raw JSON
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	// Handle special Django types first
	if idRaw, ok := raw["id"]; ok {
		var djangoID djangoUUID
		if err := json.Unmarshal(idRaw, &djangoID); err == nil && djangoID.Type == "uuid" {
			parsedID, err := uuid.Parse(djangoID.Value.Hex)
			if err != nil {
				return fmt.Errorf("invalid id UUID: %w", err)
			}
			s.ID = parsedID
		}
	}

	if createdByRaw, ok := raw["created_by"]; ok {
		var djangoCreatedBy djangoUUID
		if err := json.Unmarshal(createdByRaw, &djangoCreatedBy); err == nil && djangoCreatedBy.Type == "uuid" {
			parsedCreatedBy, err := uuid.Parse(djangoCreatedBy.Value.Hex)
			if err != nil {
				return fmt.Errorf("invalid created_by UUID: %w", err)
			}
			s.CreatedBy = parsedCreatedBy
		}
	}

	// Remove special fields from raw so they don't interfere with standard unmarshaling
	delete(raw, "id")
	delete(raw, "created_by")
	delete(raw, "created_at")
	delete(raw, "updated_at")

	// Marshal remaining fields and unmarshal into struct
	remainingData, err := json.Marshal(raw)
	if err != nil {
		return err
	}

	type Alias SpaceData
	aux := (*Alias)(s)
	return json.Unmarshal(remainingData, aux)
}

// CeleryMessage represents the raw Celery message format
// Format: [args, kwargs, metadata]
type CeleryMessage struct {
	Args     json.RawMessage `json:"-"` // Positional arguments array (index 0)
	Kwargs   json.RawMessage `json:"-"` // Keyword arguments (index 1)
	Metadata json.RawMessage `json:"-"` // Celery metadata (index 2)
}

// UnmarshalJSON parses the Celery array format
func (m *CeleryMessage) UnmarshalJSON(data []byte) error {
	var arr []json.RawMessage
	if err := json.Unmarshal(data, &arr); err != nil {
		return err
	}
	if len(arr) < 2 {
		return fmt.Errorf("invalid Celery message format: expected at least 2 elements, got %d", len(arr))
	}
	m.Args = arr[0]
	m.Kwargs = arr[1]
	if len(arr) > 2 {
		m.Metadata = arr[2]
	}
	return nil
}

// UpdateSpaceTask represents the Celery task kwargs for update_space
type UpdateSpaceTask struct {
	OrganizationSlugName string    `json:"organization_slug_name"`
	Data                 SpaceData `json:"data"`
}

// DeleteSpaceTask represents the Celery task kwargs for delete_space
type DeleteSpaceTask struct {
	OrganizationSlugName string `json:"organization_slug_name"`
	PK                   string `json:"pk"` // Space ID as string
}
