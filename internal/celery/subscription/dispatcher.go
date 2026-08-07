package subscription

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Space-DF/telemetry-service/internal/celery/taskerrors"
	"github.com/Space-DF/telemetry-service/internal/models"
	"go.uber.org/zap"
)

func (h *Handler) unmarshalTask(body []byte) (models.SubscriptionTask, error) {
	var celeryMsg models.CeleryMessage
	if err := json.Unmarshal(body, &celeryMsg); err != nil {
		return models.SubscriptionTask{}, fmt.Errorf("failed to unmarshal celery message: %w", err)
	}

	var task models.SubscriptionTask
	if err := json.Unmarshal(celeryMsg.Kwargs, &task); err != nil {
		return models.SubscriptionTask{}, fmt.Errorf("failed to unmarshal task kwargs: %w", err)
	}

	if task.OrgSlug == "" {
		return models.SubscriptionTask{}, taskerrors.NewPermanentf("org_slug is required")
	}

	if task.Limits != nil {
		limits := make(map[string]int, len(task.Limits))
		for k, v := range task.Limits {
			if v < 0 {
				return models.SubscriptionTask{}, taskerrors.NewPermanentf("limit %s must be >= 0, got %d", k, v)
			}
			limits[k] = v
		}
		task.Limits = limits
	}

	if task.DeviceIDs != nil {
		ids := make([]string, len(task.DeviceIDs))
		copy(ids, task.DeviceIDs)
		task.DeviceIDs = ids
	}

	return task, nil
}

func (h *Handler) HandleDowngrade(ctx context.Context, body []byte) error {
	task, err := h.unmarshalTask(body)
	if err != nil {
		return err
	}

	h.logger.Info("Processing telemetry subscription downgrade",
		zap.String("org", task.OrgSlug),
		zap.Int("device_count", len(task.DeviceIDs)),
		zap.Int("limit_count", len(task.Limits)))

	h.mu.RLock()
	handlers := make([]lifecycleHandler, len(h.handlers))
	copy(handlers, h.handlers)
	h.mu.RUnlock()

	for _, handler := range handlers {
		if err := handler.HandleDowngrade(ctx, task); err != nil {
			return err
		}
	}
	return nil
}

func (h *Handler) HandleUpgrade(ctx context.Context, body []byte) error {
	task, err := h.unmarshalTask(body)
	if err != nil {
		return err
	}

	h.logger.Info("Processing telemetry subscription upgrade",
		zap.String("org", task.OrgSlug),
		zap.Int("device_count", len(task.DeviceIDs)),
		zap.Int("limit_count", len(task.Limits)))

	h.mu.RLock()
	handlers := make([]lifecycleHandler, len(h.handlers))
	copy(handlers, h.handlers)
	h.mu.RUnlock()

	for _, handler := range handlers {
		if err := handler.HandleUpgrade(ctx, task); err != nil {
			return err
		}
	}
	return nil
}
