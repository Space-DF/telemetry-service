package subscription

import (
	"context"
	"sync"

	"github.com/Space-DF/telemetry-service/internal/celery/taskerrors"
	"github.com/Space-DF/telemetry-service/internal/celery/topology"
	"github.com/Space-DF/telemetry-service/internal/models"
	"github.com/Space-DF/telemetry-service/internal/timescaledb"
	"go.uber.org/zap"
)

type lifecycleHandler interface {
	HandleDowngrade(context.Context, models.SubscriptionTask) error
	HandleUpgrade(context.Context, models.SubscriptionTask) error
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
	handlers []lifecycleHandler
	mu       sync.RWMutex
}

func NewHandler(dbClient *timescaledb.Client, logger *zap.Logger) *Handler {
	h := &Handler{
		logger: logger,
	}

	h.RegisterHandler(newAutomationLifecycleHandler(dbClient, logger))
	h.RegisterHandler(newEntityLifecycleHandler(dbClient, logger))

	return h
}

// RegisterHandler adds a new handler to the dispatcher. Thread-safe.
func (h *Handler) RegisterHandler(handler lifecycleHandler) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.handlers = append(h.handlers, handler)
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
