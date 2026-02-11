package models

import "time"

// DisplayType constants
const (
	DisplayTypeGauge     = "gauge"
	DisplayTypeSlider    = "slider"
	DisplayTypeValue     = "value"
	DisplayTypeChart     = "chart"
	DisplayTypeHistogram = "histogram"
	DisplayTypeTable     = "table"
	DisplayTypeSwitch    = "switch"
	DisplayTypeMap       = "map"
)

// WidgetDataRequest represents widget data fetch request
type WidgetDataRequest struct {
	EntityID    string     `query:"entity_id"`
	OrgSlug     string     `query:"org_slug"`
	SpaceSlug   string     `query:"space_slug"`
	DeviceID    string     `query:"device_id"`
	StartTime   *time.Time `query:"start_time"`
	EndTime     *time.Time `query:"end_time"`
	DisplayType string     `query:"display_type"`
}
