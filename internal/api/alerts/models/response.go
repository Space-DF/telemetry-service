package models

// AlertsResponse represents paginated alerts response
type AlertsResponse struct {
	Results    []interface{} `json:"results"`
	TotalCount int           `json:"total_count" example:"150"`
	Page       int           `json:"page" example:"1"`
	PageSize   int           `json:"page_size" example:"20"`
}
