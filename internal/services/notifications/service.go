package notifications

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

// Service delivers device event notifications via web push.
type Service struct {
	store         SubscriptionStore
	logger        *zap.Logger
	cfg           config.Notifications
	sender        PushSender
	authClient    *client.AuthServiceClient
	mu            sync.Mutex
	deleteBackoff map[string]time.Time
}

// New creates a notification service.
func New(store SubscriptionStore, logger *zap.Logger, cfg config.Notifications) *Service {
	return &Service{
		store:         store,
		logger:        logger,
		cfg:           cfg,
		sender:        webPushClient{},
		authClient:    client.NewAuthServiceClient(logger),
		deleteBackoff: make(map[string]time.Time),
	}
}

// SetSender overrides the web push sender, mainly for tests.
func (s *Service) SetSender(sender PushSender) {
	if sender != nil {
		s.sender = sender
	}
}

// Enabled reports whether delivery is fully configured.
func (s *Service) Enabled() bool {
	return s != nil &&
		s.cfg.VAPIDPublicKey != "" &&
		s.cfg.VAPIDPrivateKey != "" &&
		s.cfg.VAPIDSubject != ""
}

func (s *Service) notify(ctx context.Context, orgSlug, spaceSlug, deviceID string, isPublic bool, payloadObj *models.PushNotificationPayload) error {
	if s.store == nil {
		return fmt.Errorf("notification store is not configured")
	}

	targetSpaceSlug := spaceSlug
	if isPublic {
		targetSpaceSlug = ""
	}

	users, err := s.authClient.GetUsers(ctx, orgSlug, targetSpaceSlug)
	if err != nil {
		s.logger.Warn("failed to fetch notification recipients from auth-service",
			zap.Error(err),
			zap.String("space_slug", spaceSlug),
			zap.String("org_slug", orgSlug),
		)
		return fmt.Errorf("fetch notification recipients: %w", err)
	}

	userIDs := make([]string, 0, len(users))
	userSpaceMap := make(map[string]string, len(users))
	for _, u := range users {
		if u.ID != "" {
			userIDs = append(userIDs, u.ID)
			if u.SlugName != "" {
				userSpaceMap[u.ID] = u.SlugName
			}
		}
	}

	s.logger.Info("fetched users for notification",
		zap.Bool("is_public", isPublic),
		zap.Int("user_count", len(userIDs)),
	)

	if len(userIDs) == 0 {
		s.logger.Info("no users found for notification",
			zap.Bool("is_public", isPublic),
			zap.String("space_slug", spaceSlug),
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

	var sendErrs []string

	for _, sub := range subscriptions {
		if sub == nil {
			s.logger.Warn("subscription is nil")
			continue
		}

		// determine per-user space slug name if available
		spaceName := ""
		if sub.UserID != "" {
			if v, ok := userSpaceMap[sub.UserID]; ok && v != "" {
				// public API returns per-user slug_name
				spaceName = v
			} else if !isPublic && spaceSlug != "" {
				// private API returns only user_ids — use provided spaceSlug for private notifications
				spaceName = spaceSlug
			}
		}

		// create a copy of the payload and ensure Data map is copied
		personalized := *payloadObj
		if personalized.Data == nil {
			personalized.Data = make(map[string]interface{})
		} else {
			newData := make(map[string]interface{}, len(personalized.Data)+1)
			for k, v := range personalized.Data {
				newData[k] = v
			}
			personalized.Data = newData
		}
		personalized.Data["space_slug"] = spaceName

		payloadBytes, err := json.Marshal(&personalized)
		if err != nil {
			s.logger.Error("failed to marshal personalized notification payload",
				zap.Error(err),
				zap.String("subscription_id", sub.ID),
			)
			sendErrs = append(sendErrs, fmt.Sprintf("subscription %s: marshal error: %v", sub.ID, err))
			continue
		}

		resp, err := s.sender.SendNotification(payloadBytes, &webpush.Subscription{
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

			sendErrs = append(sendErrs, fmt.Sprintf("subscription %s: %v", sub.ID, err))
			continue
		}

		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()

		if resp.StatusCode == http.StatusGone || resp.StatusCode == http.StatusNotFound {
			s.logger.Warn("subscription is stale, deleting",
				zap.String("subscription_id", sub.ID),
				zap.Int("status_code", resp.StatusCode),
			)
			s.tryDeleteSubscription(ctx, orgSlug, sub.UserID, sub.ID, http.StatusText(resp.StatusCode))
			continue
		}

		if resp.StatusCode >= http.StatusMultipleChoices {
			s.logger.Error("push notification failed with bad status",
				zap.String("subscription_id", sub.ID),
				zap.Int("status_code", resp.StatusCode),
			)

			sendErrs = append(sendErrs, fmt.Sprintf("subscription %s returned status %d", sub.ID, resp.StatusCode))
			continue
		}

		s.logger.Info("push notification sent successfully",
			zap.String("subscription_id", sub.ID),
		)
	}

	if len(sendErrs) == 0 {
		s.logger.Info("all notifications delivered successfully",
			zap.String("device_id", deviceID),
			zap.String("space_slug", spaceSlug),
		)
		return nil
	}

	s.logger.Error("some notifications failed",
		zap.Strings("errors", sendErrs),
	)

	return errors.New(strings.Join(sendErrs, "; "))
}

func (s *Service) tryDeleteSubscription(ctx context.Context, orgSlug, userID, subscriptionID, reason string) {
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

func (s *Service) shouldAttemptDelete(subscriptionID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	nextAllowed, ok := s.deleteBackoff[subscriptionID]
	return !ok || time.Now().After(nextAllowed)
}

func (s *Service) recordDeleteFailure(subscriptionID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.deleteBackoff[subscriptionID] = time.Now().Add(failedDeletionRetryDelay)
}

func (s *Service) clearDeleteFailure(subscriptionID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.deleteBackoff, subscriptionID)
}
