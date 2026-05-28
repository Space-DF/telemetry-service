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

// processTenantMessages processes messages for a specific tenant
func (c *MultiTenantConsumer) processTenantMessages(ctx context.Context, tenant *TenantConsumer, messages <-chan amqp.Delivery) {
	c.logger.Info("Processing messages for organization", zap.String("org", tenant.OrgSlug))

	for {
		select {
		case <-ctx.Done():
			c.logger.Info("Stopping message processing for organization", zap.String("org", tenant.OrgSlug))
			return

		case msg, ok := <-messages:
			if !ok {
				c.logger.Warn("Message channel closed for organization, draining remaining messages and triggering resubscription",
					zap.String("org", tenant.OrgSlug))

				// Drain any remaining messages in the channel (they will be requeued by RabbitMQ)
				for range messages {
					// Just drain, don't process or ACK
				}

				// Trigger resubscription for this tenant using parent context
				go c.resubscribeTenant(tenant.ParentCtx, tenant)
				return
			}

			// Check if the delivery channel is still valid before processing
			// When the AMQP connection closes, the delivery becomes invalid
			if tenant.Channel == nil || tenant.Channel.IsClosed() {
				c.logger.Warn("Tenant channel closed, skipping message processing",
					zap.String("org", tenant.OrgSlug))

				go c.resubscribeTenant(tenant.ParentCtx, tenant)
				return
			}

			kind := messageKindFromRoutingKey(msg.RoutingKey)

			orgSlug := extractOrgFromRoutingKey(msg.RoutingKey)
			if orgSlug == "" {
				orgSlug = tenant.OrgSlug
			}
			orgCtx := timescaledb.ContextWithOrg(ctx, orgSlug)

			c.logger.Debug("Received message",
				zap.String("org", orgSlug),
				zap.String("routing_key", msg.RoutingKey),
				zap.String("kind", kind))

			switch kind {
			case "entity_telemetry":
				c.handleEntityTelemetry(orgCtx, orgSlug, msg)
			case "event":
				c.handleEvent(orgCtx, orgSlug, msg)
			case "location_update":
				c.handleLocationMessage(orgCtx, orgSlug, msg)
			case "activity_log":
				c.handleActivityLog(orgCtx, orgSlug, msg)
			default:
				if ackErr := msg.Ack(false); ackErr != nil {
					c.logger.Error("Failed to ack unknown message", zap.Error(ackErr))
				}
			}
		}
	}
}

func (c *MultiTenantConsumer) handleEntityTelemetry(ctx context.Context, orgSlug string, msg amqp.Delivery) {
	var entityPayload models.EntityTelemetryPayload
	if err := json.Unmarshal(msg.Body, &entityPayload); err != nil {
		c.logger.Error("Failed to unmarshal entity telemetry",
			zap.Error(err),
			zap.String("org", orgSlug))
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

	if err := c.processor.ProcessTelemetry(ctx, telemetry); err != nil {
		c.logger.Error("Failed to process entity telemetry",
			zap.Error(err),
			zap.String("org", orgSlug))
		_ = msg.Nack(false, true)
		return
	}

	if ackErr := msg.Ack(false); ackErr != nil {
		c.logger.Error("Failed to ack entity telemetry", zap.Error(ackErr))
	}
}

func (c *MultiTenantConsumer) handleEvent(ctx context.Context, orgSlug string, msg amqp.Delivery) {
	var event models.Event
	if err := json.Unmarshal(msg.Body, &event); err != nil {
		c.logger.Error("Failed to unmarshal event",
			zap.Error(err),
			zap.String("org", orgSlug))
		_ = msg.Nack(false, false)
		return
	}

	if event.Organization == "" {
		event.Organization = orgSlug
	}

	if err := c.processor.ProcessLNSAlertEvent(ctx, &event); err != nil {
		c.logger.Error("Failed to process event",
			zap.Error(err),
			zap.String("org", orgSlug))
		_ = msg.Nack(false, true)
		return
	}

	if ackErr := msg.Ack(false); ackErr != nil {
		c.logger.Error("Failed to ack event", zap.Error(ackErr))
	}
}

func (c *MultiTenantConsumer) handleActivityLog(ctx context.Context, orgSlug string, msg amqp.Delivery) {
	var activityLog models.DeviceActivityLog
	if err := json.Unmarshal(msg.Body, &activityLog); err != nil {
		c.logger.Error("Failed to unmarshal activity log",
			zap.Error(err),
			zap.String("org", orgSlug))
		_ = msg.Nack(false, false)
		return
	}

	if activityLog.Payload == nil {
		activityLog.Payload = json.RawMessage("{}")
	}

	// Validate UUID format
	if _, err := uuid.Parse(activityLog.ID); err != nil {
		c.logger.Error("Invalid UUID in activity log id",
			zap.String("id", activityLog.ID), zap.Error(err))
		_ = msg.Nack(false, false) // discard, don't requeue
		return
	}

	if err := c.processor.ProcessActivityLog(ctx, orgSlug, &activityLog); err != nil {
		c.logger.Error("Failed to process activity log",
			zap.Error(err),
			zap.String("org", orgSlug))
		_ = msg.Nack(false, false)
		return
	}

	if ackErr := msg.Ack(false); ackErr != nil {
		c.logger.Error("Failed to ack activity log", zap.Error(ackErr))
	}
}

func (c *MultiTenantConsumer) handleLocationMessage(ctx context.Context, orgSlug string, msg amqp.Delivery) {
	var telemetry models.TelemetryPayload
	if err := json.Unmarshal(msg.Body, &telemetry); err == nil && len(telemetry.Entities) > 0 {
		if telemetry.Organization == "" {
			telemetry.Organization = orgSlug
		}
		if telemetry.SpaceSlug == "" {
			telemetry.SpaceSlug = orgSlug
		}

		if err := c.processor.ProcessTelemetry(ctx, &telemetry); err != nil {
			c.logger.Error("Failed to process telemetry payload",
				zap.Error(err),
				zap.String("org", orgSlug))
			if nackErr := msg.Nack(false, true); nackErr != nil {
				c.logger.Error("Failed to nack message", zap.Error(nackErr))
			}
			return
		}

		if ackErr := msg.Ack(false); ackErr != nil {
			c.logger.Error("Failed to ack message", zap.Error(ackErr))
		}
		return
	}

	var deviceMsg models.DeviceLocationMessage
	if err := json.Unmarshal(msg.Body, &deviceMsg); err != nil {
		c.logger.Error("Failed to unmarshal message",
			zap.Error(err),
			zap.String("org", orgSlug))
		if nackErr := msg.Nack(false, false); nackErr != nil {
			c.logger.Error("Failed to nack bad message", zap.Error(nackErr))
		}
		return
	}

	if err := c.processor.ProcessDeviceLocation(ctx, &deviceMsg); err != nil {
		c.logger.Error("Failed to process message",
			zap.Error(err),
			zap.String("org", orgSlug))
		if errors.Is(err, timescaledb.ErrLocationDroppedTimeout) {
			c.logger.Warn("Location dropped due to timeout",
				zap.String("org", orgSlug))
			if nackErr := msg.Nack(false, true); nackErr != nil {
				c.logger.Error("Failed to nack timeout message", zap.Error(nackErr))
			}
		}
	} else {
		if ackErr := msg.Ack(false); ackErr != nil {
			c.logger.Error("Failed to ack message", zap.Error(ackErr))
		}
	}
}
