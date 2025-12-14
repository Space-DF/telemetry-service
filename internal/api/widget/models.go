package widget

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

// GaugeValueResponse for gauge, slider, value types
type GaugeValueResponse struct {
	Value             float64 `json:"value"`
	UnitOfMeasurement string  `json:"unit_of_measurement,omitempty"`
}

// SwitchValueResponse for switch type
type SwitchValueResponse struct {
	Value bool `json:"value"`
}

// ChartDataPoint for chart/histogram data
type ChartDataPoint struct {
	Timestamp time.Time `json:"timestamp"`
	Value     float64   `json:"value"`
}

// ChartDataResponse for chart and histogram types
type ChartDataResponse struct {
	Data []ChartDataPoint `json:"data"`
}

// TableRow for table type
type TableRow struct {
	Timestamp time.Time              `json:"timestamp"`
	Values    map[string]interface{} `json:"values"`
}

// TableDataResponse for table type
type TableDataResponse struct {
	Columns []string   `json:"columns"`
	Data    []TableRow `json:"data"`
}

// HistogramBucket for histogram type
type HistogramBucket struct {
	Bucket string  `json:"bucket"`
	Count  int64   `json:"count"`
	Value  float64 `json:"value"`
}

// HistogramDataResponse for histogram type
type HistogramDataResponse struct {
	Data []HistogramBucket `json:"data"`
}

// Coordinate for map type
type Coordinate struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

// MapDataResponse for map type
type MapDataResponse struct {
	Coordinate Coordinate `json:"coordinate"`
}

