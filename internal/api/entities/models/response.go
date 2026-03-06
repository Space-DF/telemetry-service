package models

// EntitiesResponse represents paginated entities response
type EntitiesResponse struct {
	Count   int           					 `json:"count" example:"25"`
	Results []map[string]interface{} `json:"results"`
}
