package models

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

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
	GeofenceID uuid.UUID  `json:"geofence_id" db:"geofence_id"`
	Name       string     `json:"name" db:"name"`
	TypeZone   string     `json:"type_zone" db:"type_zone"`
	Geometry   Geometry   `json:"geometry" db:"geometry"`
	IsActive   bool       `json:"is_active" db:"is_active"`
	SpaceID    *uuid.UUID `json:"space_id,omitempty" db:"space_id"`
	CreatedAt  time.Time  `json:"created_at" db:"created_at"`
	UpdatedAt  time.Time  `json:"updated_at" db:"updated_at"`
}

// Space represents a space/organization
type Space struct {
	SpaceID      uuid.UUID  `json:"space_id" db:"space_id"`
	Name         string     `json:"name" db:"name"`
	Logo         *string    `json:"logo,omitempty" db:"logo"`
	SpaceSlug    string     `json:"space_slug" db:"space_slug"`
	IsActive     bool       `json:"is_active" db:"is_active"`
	TotalDevices *int       `json:"total_devices,omitempty" db:"total_devices"`
	Description  *string    `json:"description,omitempty" db:"description"`
	CreatedBy    *uuid.UUID `json:"created_by,omitempty" db:"created_by"`
	CreatedAt    time.Time  `json:"created_at" db:"created_at"`
}

// GeofenceWithSpace represents a geofence with its associated space details
type GeofenceWithSpace struct {
	GeofenceID uuid.UUID  `json:"geofence_id" db:"geofence_id"`
	Name       string     `json:"name" db:"name"`
	TypeZone   string     `json:"type_zone" db:"type_zone"`
	Geometry   Geometry   `json:"geometry" db:"geometry"`
	IsActive   bool       `json:"is_active" db:"is_active"`
	SpaceID    *uuid.UUID `json:"space_id,omitempty" db:"space_id"`
	CreatedAt  time.Time  `json:"created_at" db:"created_at"`
	UpdatedAt  time.Time  `json:"updated_at" db:"updated_at"`

	// Joined space fields
	SpaceName   *string `json:"space_name,omitempty" db:"space_name"`
	SpaceSlug   *string `json:"space_slug,omitempty" db:"space_slug"`
	SpaceLogo   *string `json:"space_logo,omitempty" db:"space_logo"`
}

// GeofenceDeviceRelation represents the relationship between geofences and devices via event_rules
type GeofenceDeviceRelation struct {
	GeofenceID   uuid.UUID `json:"geofence_id" db:"geofence_id"`
	GeofenceName string    `json:"geofence_name" db:"geofence_name"`
	DeviceID     uuid.UUID `json:"device_id" db:"device_id"`
	IsActive     bool      `json:"is_active" db:"is_active"`
}

// Point represents a simple point for geofence testing
type Point struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

// NewPointFromWKT creates a Point from WKT format
func NewPointFromWKT(wkt string) (*Point, error) {
	var lon, lat float64
	_, err := fmt.Sscanf(wkt, "POINT(%f %f)", &lon, &lat)
	if err != nil {
		return nil, fmt.Errorf("failed to parse WKT: %w", err)
	}
	return &Point{Latitude: lat, Longitude: lon}, nil
}

// ToWKT converts Point to WKT format
func (p *Point) ToWKT() string {
	return fmt.Sprintf("POINT(%f %f)", p.Longitude, p.Latitude)
}

// ToGeoJSON converts Point to GeoJSON format
func (p *Point) ToGeoJSON() ([]byte, error) {
	geojson := map[string]interface{}{
		"type": "Point",
		"coordinates": []float64{
			p.Longitude,
			p.Latitude,
		},
	}
	return json.Marshal(geojson)
}

// Polygon represents a polygon for geofence definition
type Polygon struct {
	Coordinates [][]Point `json:"coordinates"` // Outer ring + inner rings (holes)
}

// ToGeoJSON converts Polygon to GeoJSON format
func (p *Polygon) ToGeoJSON() ([]byte, error) {
	coordinates := make([][][]float64, len(p.Coordinates))
	for i, ring := range p.Coordinates {
		coordinates[i] = make([][]float64, len(ring))
		for j, point := range ring {
			coordinates[i][j] = []float64{point.Longitude, point.Latitude}
		}
	}
	geojson := map[string]interface{}{
		"type":        "Polygon",
		"coordinates": coordinates,
	}
	return json.Marshal(geojson)
}
