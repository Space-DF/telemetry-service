package subscription

import (
	"context"
	"fmt"

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

func hasUnlimitedFeature(features []string, feature string) bool {
	for _, item := range features {
		if item == feature {
			return true
		}
	}
	return false
}

func (h automationLifecycleHandler) HandleDowngrade(ctx context.Context, task models.SubscriptionDowngradeTask) error {
	maxActive, ok := task.Limits[automationMaxCountFeature]
	if !ok {
		return taskerrors.NewPermanentf("automation event requires %s for downgrade", automationMaxCountFeature)
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

func (h automationLifecycleHandler) HandleUpgrade(ctx context.Context, task models.SubscriptionUpgradeTask) error {
	maxActive, limited := task.Limits[automationMaxCountFeature]
	unlimited := hasUnlimitedFeature(task.UnlimitedFeatures, automationMaxCountFeature)
	if !limited && !unlimited {
		return taskerrors.NewPermanentf("automation event requires %s or explicit unlimited feature for upgrade", automationMaxCountFeature)
	}
	if limited && maxActive < 0 {
		return taskerrors.NewPermanentf("%s must be >= 0", automationMaxCountFeature)
	}

	var (
		reactivated int64
		err         error
	)
	if limited {
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
