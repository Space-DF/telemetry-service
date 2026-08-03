package subscription

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Space-DF/telemetry-service/internal/celery/taskerrors"
	"github.com/Space-DF/telemetry-service/internal/models"
	"go.uber.org/zap"
)

func (h *Handler) HandleDowngrade(ctx context.Context, body []byte) error {
	var celeryMsg models.CeleryMessage
	if err := json.Unmarshal(body, &celeryMsg); err != nil {
		return fmt.Errorf("failed to unmarshal celery message: %w", err)
	}

	var task models.SubscriptionDowngradeTask
	if err := json.Unmarshal(celeryMsg.Kwargs, &task); err != nil {
		return fmt.Errorf("failed to unmarshal telemetry_downgrade task kwargs: %w", err)
	}

	h.logger.Info("Processing telemetry subscription downgrade",
		zap.String("org", task.OrgSlug),
		zap.String("event", string(task.Event)),
		zap.Int("device_count", len(task.DeviceIDs)))

	handler, ok := h.handlers[task.Event]
	if !ok {
		return taskerrors.NewPermanentf("unsupported telemetry subscription downgrade event: %s", task.Event)
	}

	return handler.HandleDowngrade(ctx, task)
}

func (h *Handler) HandleUpgrade(ctx context.Context, body []byte) error {
	var celeryMsg models.CeleryMessage
	if err := json.Unmarshal(body, &celeryMsg); err != nil {
		return fmt.Errorf("failed to unmarshal celery message: %w", err)
	}

	var task models.SubscriptionUpgradeTask
	if err := json.Unmarshal(celeryMsg.Kwargs, &task); err != nil {
		return fmt.Errorf("failed to unmarshal telemetry_upgrade task kwargs: %w", err)
	}

	h.logger.Info("Processing telemetry subscription upgrade",
		zap.String("org", task.OrgSlug),
		zap.String("event", string(task.Event)),
		zap.Int("device_count", len(task.DeviceIDs)))

	handler, ok := h.handlers[task.Event]
	if !ok {
		return taskerrors.NewPermanentf("unsupported telemetry subscription upgrade event: %s", task.Event)
	}

	return handler.HandleUpgrade(ctx, task)
}
