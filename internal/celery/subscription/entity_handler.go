package subscription

import (
	"context"
	"fmt"

	"github.com/Space-DF/telemetry-service/internal/models"
	"github.com/Space-DF/telemetry-service/internal/timescaledb"
	"go.uber.org/zap"
)

type entityLifecycleHandler struct {
	dbClient *timescaledb.Client
	logger   *zap.Logger
}

func newEntityLifecycleHandler(dbClient *timescaledb.Client, logger *zap.Logger) entityLifecycleHandler {
	return entityLifecycleHandler{dbClient: dbClient, logger: logger}
}

func (h entityLifecycleHandler) HandleDowngrade(ctx context.Context, task models.SubscriptionTask) error {
	if len(task.DeviceIDs) == 0 {
		// Need optimization later
		return nil
	}

	deactivated, err := h.dbClient.BulkDeactivateEntities(ctx, task.OrgSlug, task.DeviceIDs)
	if err != nil {
		return fmt.Errorf("failed to bulk deactivate entities: %w", err)
	}

	h.logger.Info("Entity downgrade completed",
		zap.String("org", task.OrgSlug),
		zap.Int("device_count", len(task.DeviceIDs)),
		zap.Int64("deactivated", deactivated))
	return nil
}

func (h entityLifecycleHandler) HandleUpgrade(ctx context.Context, task models.SubscriptionTask) error {
	if len(task.DeviceIDs) == 0 {
		// Need optimization later
		return nil
	}

	reactivated, err := h.dbClient.BulkReactivateEntitiesByDeviceIDs(ctx, task.OrgSlug, task.DeviceIDs)
	if err != nil {
		return fmt.Errorf("failed to bulk reactivate entities: %w", err)
	}

	h.logger.Info("Entity upgrade completed",
		zap.String("org", task.OrgSlug),
		zap.Int("device_count", len(task.DeviceIDs)),
		zap.Int64("reactivated", reactivated))
	return nil
}
