package models

// PushSubscriptionRequest represents a request to subscribe to push notifications
type PushSubscriptionRequest struct {
	Endpoint string `json:"endpoint" validate:"required,url"`
	P256DH   string `json:"p256dh" validate:"required"`
	Auth     string `json:"auth" validate:"required"`
}
