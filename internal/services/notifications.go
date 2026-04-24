package services

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/SherClockHolmes/webpush-go"
	"github.com/Space-DF/telemetry-service/internal/client"
	"github.com/Space-DF/telemetry-service/internal/config"
	"github.com/Space-DF/telemetry-service/internal/models"
	"go.uber.org/zap"
)

const failedDeletionRetryDelay = 15 * time.Minute

// SubscriptionStore provides access to push subscriptions.
type SubscriptionStore interface {
	GetPushSubscriptionsByUserIDs(ctx context.Context, orgSlug string, userIDs []string) ([]*models.PushSubscription, error)
	DeletePushSubscriptionById(ctx context.Context, orgSlug, userID, subscriptionID string) error
}

// PushSender sends web push payloads to browser subscriptions.
type PushSender interface {
	SendNotification(payload []byte, subscription *webpush.Subscription, options *webpush.Options) (*http.Response, error)
}

type webPushClient struct{}

func (webPushClient) SendNotification(payload []byte, subscription *webpush.Subscription, options *webpush.Options) (*http.Response, error) {
	return webpush.SendNotification(payload, subscription, options)
}

// NotificationService delivers device event notifications via web push.
type NotificationService struct {
	store         SubscriptionStore
	logger        *zap.Logger
	cfg           config.Notifications
	sender        PushSender
	authClient    *client.AuthServiceClient
	mu            sync.Mutex
	deleteBackoff map[string]time.Time
}

// NewNotificationService creates a notification service.
func NewNotificationService(store SubscriptionStore, logger *zap.Logger, cfg config.Notifications) *NotificationService {
	return &NotificationService{
		store:         store,
		logger:        logger,
		cfg:           cfg,
		sender:        webPushClient{},
		authClient:    client.NewAuthServiceClient(logger),
		deleteBackoff: make(map[string]time.Time),
	}
}

// SetSender overrides the web push sender, mainly for tests.
func (s *NotificationService) SetSender(sender PushSender) {
	if sender != nil {
		s.sender = sender
	}
}

// Enabled reports whether delivery is fully configured.
func (s *NotificationService) Enabled() bool {
	return s != nil &&
		s.cfg.VAPIDPublicKey != "" &&
		s.cfg.VAPIDPrivateKey != "" &&
		s.cfg.VAPIDSubject != ""
}

// NewDeviceEventNotification creates a new notification from an event.
func NewDeviceEventNotification(event *models.Event) *models.DeviceEventNotification {
	return &models.DeviceEventNotification{
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

func (s *NotificationService) NotifyEvent(ctx context.Context, event *models.Event, orgSlug string) error {
	if s == nil || !s.Enabled() || event == nil || strings.TrimSpace(event.SpaceSlug) == "" {
		return nil
	}

	if s.store == nil {
		return fmt.Errorf("notification store is not configured")
	}

	s.logger.Info("starting event notification",
		zap.String("org_slug", orgSlug),
		zap.String("space_slug", event.SpaceSlug),
		zap.String("device_id", event.DeviceID),
	)

	userIDs, err := s.authClient.GetSpaceUserIDs(ctx, orgSlug, event.SpaceSlug)
	if err != nil {
		s.logger.Warn("failed to fetch space users from auth-service",
			zap.Error(err),
			zap.String("space_slug", event.SpaceSlug),
			zap.String("org_slug", orgSlug),
		)
		return fmt.Errorf("fetch space users: %w", err)
	}

	s.logger.Info("fetched users for notification",
		zap.Int("user_count", len(userIDs)),
		zap.Strings("user_ids", userIDs),
	)

	if len(userIDs) == 0 {
		s.logger.Info("no users found for space",
			zap.String("space_slug", event.SpaceSlug),
		)
		return nil
	}

	subscriptions, err := s.store.GetPushSubscriptionsByUserIDs(ctx, orgSlug, userIDs)
	if err != nil {
		s.logger.Error("failed to load push subscriptions",
			zap.Error(err),
			zap.String("org_slug", orgSlug),
		)
		return fmt.Errorf("load subscriptions: %w", err)
	}

	s.logger.Info("loaded push subscriptions",
		zap.Int("subscription_count", len(subscriptions)),
	)

	if len(subscriptions) == 0 {
		s.logger.Info("no push subscriptions found")
		return nil
	}

	payloadObj := NewDeviceEventNotification(event)

	payload, err := json.Marshal(payloadObj)
	if err != nil {
		s.logger.Error("failed to marshal notification payload",
			zap.Error(err),
		)
		return fmt.Errorf("marshal notification payload: %w", err)
	}

	s.logger.Debug("notification payload",
		zap.ByteString("payload", payload),
	)

	var sendErrs []string

	for _, sub := range subscriptions {
		if sub == nil {
			s.logger.Warn("subscription is nil")
			continue
		}

		resp, err := s.sender.SendNotification(payload, &webpush.Subscription{
			Endpoint: sub.Endpoint,
			Keys: webpush.Keys{
				P256dh: sub.P256DH,
				Auth:   sub.Auth,
			},
		}, &webpush.Options{
			Subscriber:      s.cfg.VAPIDSubject,
			VAPIDPublicKey:  s.cfg.VAPIDPublicKey,
			VAPIDPrivateKey: s.cfg.VAPIDPrivateKey,
			TTL:             s.cfg.TTLSeconds,
		})

		if err != nil {
			s.logger.Error("failed to send push notification",
				zap.Error(err),
				zap.String("subscription_id", sub.ID),
				zap.String("endpoint", sub.Endpoint),
			)

			sendErrs = append(sendErrs,
				fmt.Sprintf("subscription %s: %v", sub.ID, err),
			)

			continue
		}

		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()

		if resp.StatusCode == http.StatusGone ||
			resp.StatusCode == http.StatusNotFound {

			s.logger.Warn("subscription is stale, deleting",
				zap.String("subscription_id", sub.ID),
				zap.Int("status_code", resp.StatusCode),
			)

			s.tryDeleteSubscription(ctx, orgSlug, sub.UserID, sub.ID, "stale")
			continue
		}

		if resp.StatusCode >= http.StatusMultipleChoices {
			s.logger.Error("push notification failed with bad status",
				zap.String("subscription_id", sub.ID),
				zap.Int("status_code", resp.StatusCode),
			)

			sendErrs = append(sendErrs,
				fmt.Sprintf("subscription %s returned status %d",
					sub.ID,
					resp.StatusCode,
				),
			)

			continue
		}

		s.logger.Info("push notification sent successfully",
			zap.String("subscription_id", sub.ID),
		)
	}

	if len(sendErrs) == 0 {
		s.logger.Info("all notifications delivered successfully",
			zap.Any("event_id", event),
		)
		return nil
	}

	s.logger.Error("some notifications failed",
		zap.Strings("errors", sendErrs),
	)

	return errors.New(strings.Join(sendErrs, "; "))
}

func (s *NotificationService) tryDeleteSubscription(ctx context.Context, orgSlug, userID, subscriptionID, reason string) {
	if s == nil || s.store == nil || strings.TrimSpace(subscriptionID) == "" {
		return
	}
	if !s.shouldAttemptDelete(subscriptionID) {
		if s.logger != nil {
			s.logger.Debug("Skipping push subscription deletion due to retry backoff",
				zap.String("subscription_id", subscriptionID),
				zap.String("org", orgSlug),
				zap.String("reason", reason))
		}
		return
	}

	if deleteErr := s.store.DeletePushSubscriptionById(ctx, orgSlug, userID, subscriptionID); deleteErr != nil {
		s.recordDeleteFailure(subscriptionID)
		if s.logger != nil {
			s.logger.Debug("Failed to delete push subscription",
				zap.String("subscription_id", subscriptionID),
				zap.String("org", orgSlug),
				zap.String("reason", reason),
				zap.Error(deleteErr))
		}
		return
	}

	s.clearDeleteFailure(subscriptionID)
}

func (s *NotificationService) shouldAttemptDelete(subscriptionID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	nextAllowed, ok := s.deleteBackoff[subscriptionID]
	return !ok || time.Now().After(nextAllowed)
}

func (s *NotificationService) recordDeleteFailure(subscriptionID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.deleteBackoff[subscriptionID] = time.Now().Add(failedDeletionRetryDelay)
}

func (s *NotificationService) clearDeleteFailure(subscriptionID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.deleteBackoff, subscriptionID)
}
