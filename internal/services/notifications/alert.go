package notifications

import (
	"context"
	"strings"

	"github.com/Space-DF/telemetry-service/internal/models"
)

// Alert represents the input required to deliver an alert push notification.
type Alert struct {
	ID        string
	Title     string
	DeviceID  string
	SpaceSlug string
	IsPublic  bool
	Level     string
	Message   string
	Timestamp int64
}

// NewAlertPushNotification creates a push payload from an alert.
func NewAlertPushNotification(alert *Alert) *models.PushNotificationPayload {
	if alert == nil {
		return nil
	}

	level := alert.Level
	return &models.PushNotificationPayload{
		ID:         alert.ID,
		Title:      alert.Title,
		EventType:  "alert",
		DeviceID:   alert.DeviceID,
		EventLevel: &level,
		Message:    alert.Message,
		Timestamp:  alert.Timestamp,
		Data: map[string]interface{}{
			"alert_id":   alert.ID,
			"device_id":  alert.DeviceID,
			"event_type": "alert",
			"space_slug": alert.SpaceSlug,
		},
	}
}

// NotifyAlert delivers push notifications for LNS alert events.
func (s *Service) NotifyAlert(ctx context.Context, alert *Alert, orgSlug string) error {
	if s == nil || !s.Enabled() || alert == nil {
		return nil
	}
	isPublic := alert.IsPublic || strings.TrimSpace(alert.SpaceSlug) == ""
	if !isPublic && strings.TrimSpace(alert.SpaceSlug) == "" {
		return nil
	}

	payloadObj := NewAlertPushNotification(alert)
	return s.notify(ctx, orgSlug, alert.SpaceSlug, alert.DeviceID, isPublic, payloadObj)
}
