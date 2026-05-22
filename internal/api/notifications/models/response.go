package models

import "time"

// PushSubscriptionResponse represents a subscription response
type PushSubscriptionResponse struct {
	ID        string    `json:"id"`
	Endpoint  string    `json:"endpoint"`
	CreatedAt time.Time `json:"created_at"`
}
