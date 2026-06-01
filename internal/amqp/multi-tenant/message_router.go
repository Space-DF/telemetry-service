package amqp

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/Space-DF/telemetry-service/internal/models"
	"github.com/Space-DF/telemetry-service/internal/timescaledb"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

type MessageRouter struct {
	processor MessageProcessor
	logger    *zap.Logger
}

func NewMessageRouter(processor MessageProcessor, logger *zap.Logger) *MessageRouter {
	return &MessageRouter{processor: processor, logger: logger}
}

func (r *MessageRouter) RouteMessage(ctx context.Context, orgSlug string, tenant *TenantConsumer, msg amqp.Delivery) {
	if tenant.Channel == nil || tenant.Channel.IsClosed() {
		r.logger.Warn("Tenant channel closed, skipping message processing",
			zap.String("org", orgSlug))
		return
	}

	kind := messageKindFromRoutingKey(msg.RoutingKey)

	r.logger.Debug("Received message",
		zap.String("org", orgSlug),
		zap.String("routing_key", msg.RoutingKey),
		zap.String("kind", kind))

	switch kind {
	case "entity_telemetry":
		r.handleEntityTelemetry(ctx, orgSlug, msg)
	case "event":
		r.handleEvent(ctx, orgSlug, msg)
	case "location_update":
		r.handleLocationMessage(ctx, orgSlug, msg)
	case "activity_log":
		r.handleActivityLog(ctx, orgSlug, msg)
	default:
		r.logger.Warn("Unknown message kind, discarding",
			zap.String("routing_key", msg.RoutingKey),
			zap.String("kind", kind))
		if ackErr := msg.Ack(false); ackErr != nil {
			r.logger.Error("Failed to ack unknown message", zap.Error(ackErr))
		}
	}
}

func (r *MessageRouter) handleEntityTelemetry(ctx context.Context, orgSlug string, msg amqp.Delivery) {
	var entityPayload models.EntityTelemetryPayload
	if err := json.Unmarshal(msg.Body, &entityPayload); err != nil {
		r.logger.Error("Failed to unmarshal entity telemetry",
			zap.Error(err), zap.String("org", orgSlug))
		_ = msg.Nack(false, false)
		return
	}

	telemetry := &models.TelemetryPayload{
		Organization: entityPayload.Organization,
		DeviceEUI:    entityPayload.DeviceEUI,
		DeviceID:     entityPayload.DeviceID,
		SpaceSlug:    entityPayload.SpaceSlug,
		Entities:     []models.TelemetryEntity{entityPayload.Entity},
		Timestamp:    entityPayload.Timestamp,
		Source:       entityPayload.Source,
		Metadata:     entityPayload.Metadata,
	}
	if telemetry.Organization == "" {
		telemetry.Organization = orgSlug
	}

	if err := r.processor.ProcessTelemetry(ctx, telemetry); err != nil {
		r.logger.Error("Failed to process entity telemetry",
			zap.Error(err), zap.String("org", orgSlug))
		_ = msg.Nack(false, true)
		return
	}

	if ackErr := msg.Ack(false); ackErr != nil {
		r.logger.Error("Failed to ack entity telemetry", zap.Error(ackErr))
	}
}

func (r *MessageRouter) handleEvent(ctx context.Context, orgSlug string, msg amqp.Delivery) {
	var event models.Event
	if err := json.Unmarshal(msg.Body, &event); err != nil {
		r.logger.Error("Failed to unmarshal event",
			zap.Error(err), zap.String("org", orgSlug))
		_ = msg.Nack(false, false)
		return
	}

	if event.Organization == "" {
		event.Organization = orgSlug
	}

	if err := r.processor.ProcessLNSAlertEvent(ctx, &event); err != nil {
		r.logger.Error("Failed to process event",
			zap.Error(err), zap.String("org", orgSlug))
		_ = msg.Nack(false, true)
		return
	}

	if ackErr := msg.Ack(false); ackErr != nil {
		r.logger.Error("Failed to ack event", zap.Error(ackErr))
	}
}

func (r *MessageRouter) handleActivityLog(ctx context.Context, orgSlug string, msg amqp.Delivery) {
	var activityLog models.DeviceActivityLog
	if err := json.Unmarshal(msg.Body, &activityLog); err != nil {
		r.logger.Error("Failed to unmarshal activity log",
			zap.Error(err), zap.String("org", orgSlug))
		_ = msg.Nack(false, false)
		return
	}

	if activityLog.Payload == nil {
		activityLog.Payload = json.RawMessage("{}")
	}

	if _, err := uuid.Parse(activityLog.ID); err != nil {
		r.logger.Error("Invalid UUID in activity log id",
			zap.String("id", activityLog.ID), zap.Error(err))
		_ = msg.Nack(false, false)
		return
	}

	if err := r.processor.ProcessActivityLog(ctx, orgSlug, &activityLog); err != nil {
		r.logger.Error("Failed to process activity log",
			zap.Error(err), zap.String("org", orgSlug))
		_ = msg.Nack(false, false)
		return
	}

	if ackErr := msg.Ack(false); ackErr != nil {
		r.logger.Error("Failed to ack activity log", zap.Error(ackErr))
	}
}

func (r *MessageRouter) handleLocationMessage(ctx context.Context, orgSlug string, msg amqp.Delivery) {
	var telemetry models.TelemetryPayload
	if err := json.Unmarshal(msg.Body, &telemetry); err == nil && len(telemetry.Entities) > 0 {
		if telemetry.Organization == "" {
			telemetry.Organization = orgSlug
		}
		if telemetry.SpaceSlug == "" {
			telemetry.SpaceSlug = orgSlug
		}

		if err := r.processor.ProcessTelemetry(ctx, &telemetry); err != nil {
			r.logger.Error("Failed to process telemetry payload",
				zap.Error(err), zap.String("org", orgSlug))
			if nackErr := msg.Nack(false, true); nackErr != nil {
				r.logger.Error("Failed to nack message", zap.Error(nackErr))
			}
			return
		}

		if ackErr := msg.Ack(false); ackErr != nil {
			r.logger.Error("Failed to ack message", zap.Error(ackErr))
		}
		return
	}

	var deviceMsg models.DeviceLocationMessage
	if err := json.Unmarshal(msg.Body, &deviceMsg); err != nil {
		r.logger.Error("Failed to unmarshal message",
			zap.Error(err), zap.String("org", orgSlug))
		if nackErr := msg.Nack(false, false); nackErr != nil {
			r.logger.Error("Failed to nack bad message", zap.Error(nackErr))
		}
		return
	}

	if err := r.processor.ProcessDeviceLocation(ctx, &deviceMsg); err != nil {
		r.logger.Error("Failed to process message",
			zap.Error(err), zap.String("org", orgSlug))
		if errors.Is(err, timescaledb.ErrLocationDroppedTimeout) {
			r.logger.Warn("Location dropped due to timeout",
				zap.String("org", orgSlug))
			if nackErr := msg.Nack(false, true); nackErr != nil {
				r.logger.Error("Failed to nack timeout message", zap.Error(nackErr))
			}
		}
	} else {
		if ackErr := msg.Ack(false); ackErr != nil {
			r.logger.Error("Failed to ack message", zap.Error(ackErr))
		}
	}
}
