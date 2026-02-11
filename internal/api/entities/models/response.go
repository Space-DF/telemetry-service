package models

// EntitiesResponse represents paginated entities response
type EntitiesResponse struct {
	Count   int           `json:"count"`
	Results []map[string]interface{} `json:"results"`
}
