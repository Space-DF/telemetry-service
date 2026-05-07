package models

import (
	"time"
)

// PushSubscription represents a user's web push notification subscription
type PushSubscription struct {
	ID        string    `json:"id" db:"id"`
	UserID    string    `json:"user_id" db:"user_id"`
	Endpoint  string    `json:"endpoint" db:"endpoint"`
	P256DH    string    `json:"p256dh" db:"p256dh"`
	Auth      string    `json:"auth" db:"auth"`
	SpaceID   *string   `json:"space_id,omitempty" db:"space_id"`
	CreatedAt time.Time `json:"created_at" db:"created_at"`
	UpdatedAt time.Time `json:"updated_at" db:"updated_at"`
}

// DeviceEventNotification represents a web push notification for a device event
type DeviceEventNotification struct {
	ID         string                 `json:"id"`
	Title      string                 `json:"title"`
	EventType  string                 `json:"event_type"`
	DeviceID   string                 `json:"device_id"`
	EventLevel *string                `json:"event_level,omitempty"`
	Message    string                 `json:"message"`
	Timestamp  int64                  `json:"timestamp"`
	Data       map[string]interface{} `json:"data,omitempty"`
}
