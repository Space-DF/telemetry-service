package amqp

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/Space-DF/telemetry-service/internal/models"
	"github.com/Space-DF/telemetry-service/internal/timescaledb"
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

			c.logger.Debug("Received message from organization",
				zap.String("org", tenant.OrgSlug),
				zap.String("routing_key", msg.RoutingKey))

			orgCtx := timescaledb.ContextWithOrg(ctx, tenant.OrgSlug)

			// First try telemetry payload (entities).
			var telemetry models.TelemetryPayload
			if err := json.Unmarshal(msg.Body, &telemetry); err == nil && len(telemetry.Entities) > 0 {
				// Fill org if missing.
				if telemetry.Organization == "" {
					telemetry.Organization = tenant.OrgSlug
				}
				if telemetry.SpaceSlug == "" {
					telemetry.SpaceSlug = tenant.OrgSlug
				}

				if err := c.processor.ProcessTelemetry(orgCtx, &telemetry); err != nil {
					c.logger.Error("Failed to process telemetry payload",
						zap.Error(err),
						zap.String("org", tenant.OrgSlug))
					if nackErr := msg.Nack(false, true); nackErr != nil {
						c.logger.Error("Failed to nack message", zap.Error(nackErr))
					}
					continue
				}

				if ackErr := msg.Ack(false); ackErr != nil {
					c.logger.Error("Failed to ack message", zap.Error(ackErr))
				}
				continue
			}

			// Fallback to legacy device location message.
			var deviceMsg models.DeviceLocationMessage
			if err := json.Unmarshal(msg.Body, &deviceMsg); err != nil {
				c.logger.Error("Failed to unmarshal message",
					zap.Error(err),
					zap.String("org", tenant.OrgSlug))
				if nackErr := msg.Nack(false, false); nackErr != nil {
					c.logger.Error("Failed to nack bad message", zap.Error(nackErr))
				}
				continue
			}

			if err := c.processor.ProcessMessage(orgCtx, &deviceMsg); err != nil {
				c.logger.Error("Failed to process message",
					zap.Error(err),
					zap.String("org", tenant.OrgSlug))
				if errors.Is(err, timescaledb.ErrLocationDroppedTimeout) {
					c.logger.Warn("Location dropped due to timeout",
						zap.String("org", tenant.OrgSlug))
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
	}
}
