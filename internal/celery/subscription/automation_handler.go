package subscription

import (
	"context"
	"fmt"

	"slices"

	"github.com/Space-DF/telemetry-service/internal/celery/taskerrors"
	"github.com/Space-DF/telemetry-service/internal/models"
	"github.com/Space-DF/telemetry-service/internal/timescaledb"
	"go.uber.org/zap"
)

const automationMaxCountFeature = "automation.max_count"

type automationLifecycleHandler struct {
	dbClient *timescaledb.Client
	logger   *zap.Logger
}

func newAutomationLifecycleHandler(dbClient *timescaledb.Client, logger *zap.Logger) automationLifecycleHandler {
	return automationLifecycleHandler{dbClient: dbClient, logger: logger}
}

func (h automationLifecycleHandler) HandleDowngrade(ctx context.Context, task models.SubscriptionTask) error {
	maxActive, ok := task.Limits[automationMaxCountFeature]
	if !ok {
		return nil
	}
	if maxActive < 0 {
		return taskerrors.NewPermanentf("%s must be >= 0", automationMaxCountFeature)
	}

	deactivated, err := h.dbClient.BulkDeactivateAutomations(ctx, task.OrgSlug, maxActive)
	if err != nil {
		return fmt.Errorf("failed to bulk deactivate automations: %w", err)
	}
	h.logger.Info("Automation downgrade completed",
		zap.String("org", task.OrgSlug),
		zap.Int("max_active", maxActive),
		zap.Int64("deactivated", deactivated))
	return nil
}

func (h automationLifecycleHandler) HandleUpgrade(ctx context.Context, task models.SubscriptionTask) error {
	maxActive, hasLimit := task.Limits[automationMaxCountFeature]
	unlimited := slices.Contains(task.UnlimitedFeatures, automationMaxCountFeature)
	if !hasLimit && !unlimited {
		return nil
	}
	if hasLimit && maxActive < 0 {
		return taskerrors.NewPermanentf("%s must be >= 0", automationMaxCountFeature)
	}

	var (
		reactivated int64
		err         error
	)

	if hasLimit {
		reactivated, err = h.dbClient.BulkReactivateAutomationsUpToLimit(ctx, task.OrgSlug, maxActive)
	} else {
		reactivated, err = h.dbClient.BulkReactivateAutomations(ctx, task.OrgSlug)
	}

	if err != nil {
		return fmt.Errorf("failed to bulk reactivate automations: %w", err)
	}

	h.logger.Info("Automation upgrade completed",
		zap.String("org", task.OrgSlug),
		zap.Int64("reactivated", reactivated))
	return nil
}
