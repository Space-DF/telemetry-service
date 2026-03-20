package models

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
)

// ErrGeofenceNotFound is returned when a geofence is not found
var ErrGeofenceNotFound = errors.New("geofence not found")

// Geometry represents PostGIS geometry as GeoJSON
type Geometry json.RawMessage

// Value implements sql.Valuer interface
func (g Geometry) Value() (driver.Value, error) {
	if len(g) == 0 {
		return nil, nil
	}
	return string(g), nil
}

// Scan implements sql.Scanner interface
func (g *Geometry) Scan(value interface{}) error {
	if value == nil {
		*g = nil
		return nil
	}
	bytes, ok := value.([]byte)
	if !ok {
		return errors.New("failed to unmarshal Geometry value")
	}
	*g = bytes
	return nil
}

// MarshalJSON implements json.Marshaler interface
func (g Geometry) MarshalJSON() ([]byte, error) {
	if len(g) == 0 {
		return []byte("null"), nil
	}
	return []byte(g), nil
}

// UnmarshalJSON implements json.Unmarshaler interface
func (g *Geometry) UnmarshalJSON(data []byte) error {
	if g == nil {
		return errors.New("Geometry: UnmarshalJSON on nil pointer")
	}
	*g = append((*g)[:0], data...)
	return nil
}

// Geofence represents a geofence boundary
type Geofence struct {
	GeofenceID  uuid.UUID         `json:"geofence_id" db:"geofence_id"`
	Name        string            `json:"name" db:"name"`
	TypeZone    string            `json:"type_zone" db:"type_zone"`
	Features    []json.RawMessage `json:"features" db:"features"`
	Color       string            `json:"color" db:"color"`
	IsActive    bool              `json:"is_active" db:"is_active"`
	SpaceID     *uuid.UUID        `json:"space_id,omitempty" db:"space_id"`
	EventRuleID *uuid.UUID        `json:"event_rule_id,omitempty" db:"event_rule_id"`
	CreatedAt   time.Time         `json:"created_at" db:"created_at"`
	UpdatedAt   time.Time         `json:"updated_at" db:"updated_at"`
}

// GeofenceWithSpace represents a geofence with its associated space details
type GeofenceWithSpace struct {
	GeofenceID  uuid.UUID         `json:"geofence_id" db:"geofence_id"`
	Name        string            `json:"name" db:"name"`
	TypeZone    string            `json:"type_zone" db:"type_zone"`
	Features    []json.RawMessage `json:"features" db:"features"`
	Color       string            `json:"color" db:"color"`
	IsActive    bool              `json:"is_active" db:"is_active"`
	SpaceID     *uuid.UUID        `json:"space_id,omitempty" db:"space_id"`
	EventRuleID *uuid.UUID        `json:"event_rule_id,omitempty" db:"event_rule_id"`
	CreatedAt   time.Time         `json:"created_at" db:"created_at"`
	UpdatedAt   time.Time         `json:"updated_at" db:"updated_at"`
}

// DeviceGeofenceCheck holds the result of a spatial check between a device's last location and a geofence.
type DeviceGeofenceCheck struct {
	DeviceID   string
	Latitude   float64
	Longitude  float64
	IsInside   bool
	ReportedAt sql.NullTime
	DistanceKm float64
}
