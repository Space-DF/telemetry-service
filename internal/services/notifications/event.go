package notifications

import (
	"context"
	"fmt"
	"strings"

	"github.com/Space-DF/telemetry-service/internal/models"
)

// NewEventPushNotification creates a push payload from an event.
func NewEventPushNotification(event *models.Event) *models.PushNotificationPayload {
	return &models.PushNotificationPayload{
		ID:         fmt.Sprintf("%d", event.EventID),
		Title:      event.Title,
		EventType:  event.EventType,
		DeviceID:   event.DeviceID,
		EventLevel: event.EventLevel,
		Message:    event.Title,
		Timestamp:  event.TimeFiredTs,
		Data: map[string]interface{}{
			"event_id":   event.EventID,
			"device_id":  event.DeviceID,
			"event_type": event.EventType,
			"space_slug": event.SpaceSlug,
			"automation": event.AutomationName,
			"geofence":   event.GeofenceName,
			"rule_id":    event.EventRuleID,
		},
	}
}

// NotifyEvent delivers push notifications for events.
func (s *Service) NotifyEvent(ctx context.Context, event *models.Event, orgSlug string) error {
	if s == nil || !s.Enabled() || event == nil {
		return nil
	}
	isPublic := event.IsPublic || strings.TrimSpace(event.SpaceSlug) == ""
	if !isPublic && strings.TrimSpace(event.SpaceSlug) == "" {
		return nil
	}

	payloadObj := NewEventPushNotification(event)
	return s.notify(ctx, orgSlug, event.SpaceSlug, event.DeviceID, isPublic, payloadObj)
}
