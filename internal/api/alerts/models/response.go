package models

// AlertsResponse represents paginated alerts response
type AlertsResponse struct {
	Results    []interface{} `json:"results"`
	TotalCount int           `json:"total_count"`
	Page       int           `json:"page"`
	PageSize   int           `json:"page_size"`
}
