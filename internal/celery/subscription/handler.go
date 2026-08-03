package subscription

import (
	"context"

	"github.com/Space-DF/telemetry-service/internal/celery/taskerrors"
	"github.com/Space-DF/telemetry-service/internal/celery/topology"
	"github.com/Space-DF/telemetry-service/internal/models"
	"github.com/Space-DF/telemetry-service/internal/timescaledb"
	"go.uber.org/zap"
)

type lifecycleHandler interface {
	HandleDowngrade(context.Context, models.SubscriptionDowngradeTask) error
	HandleUpgrade(context.Context, models.SubscriptionUpgradeTask) error
}

const (
	TelemetryDowngradeTaskName = "spacedf.tasks.telemetry_downgrade"
	TelemetryUpgradeTaskName   = "spacedf.tasks.telemetry_upgrade"

	subscriptionDowngradeExchange = "subscription_downgrade"
	subscriptionUpgradeExchange   = "subscription_upgrade"
	telemetryDowngradeRoutingKey  = "telemetry.downgrade"
	telemetryUpgradeRoutingKey    = "telemetry.upgrade"
)

type Handler struct {
	logger   *zap.Logger
	handlers map[models.SubscriptionEvent]lifecycleHandler
}

func NewHandler(dbClient *timescaledb.Client, logger *zap.Logger) *Handler {
	return &Handler{
		logger: logger,
		handlers: map[models.SubscriptionEvent]lifecycleHandler{
			models.SubscriptionEventAutomation: newAutomationLifecycleHandler(dbClient, logger),
			models.SubscriptionEventEntities:   newEntityLifecycleHandler(dbClient, logger),
		},
	}
}

func (h *Handler) TaskNames() []string {
	return []string{
		TelemetryDowngradeTaskName,
		TelemetryUpgradeTaskName,
	}
}

func (h *Handler) QueueSpecs() []topology.Spec {
	return []topology.Spec{
		{
			Exchange:     subscriptionDowngradeExchange,
			ExchangeType: "direct",
			Queue:        "telemetry_downgrade",
			RoutingKey:   telemetryDowngradeRoutingKey,
			ConsumerTag:  "telemetry_downgrade_consumer",
			TaskName:     TelemetryDowngradeTaskName,
		},
		{
			Exchange:     subscriptionUpgradeExchange,
			ExchangeType: "direct",
			Queue:        "telemetry_upgrade",
			RoutingKey:   telemetryUpgradeRoutingKey,
			ConsumerTag:  "telemetry_upgrade_consumer",
			TaskName:     TelemetryUpgradeTaskName,
		},
	}
}

func (h *Handler) Handle(ctx context.Context, taskName string, body []byte) error {
	switch taskName {
	case TelemetryDowngradeTaskName:
		return h.HandleDowngrade(ctx, body)
	case TelemetryUpgradeTaskName:
		return h.HandleUpgrade(ctx, body)
	default:
		return taskerrors.NewPermanentf("unsupported telemetry subscription task: %s", taskName)
	}
}
