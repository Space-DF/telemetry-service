package amqp

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/Space-DF/telemetry-service/internal/models"
	"github.com/Space-DF/telemetry-service/internal/timescaledb"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

func (c *MultiTenantConsumer) routeMessage(orgCtx context.Context, tenant *TenantConsumer, msg amqp.Delivery) {
	switch {
	case strings.HasSuffix(msg.RoutingKey, ".raw_data_log"):
		c.processActivityLogMessage(orgCtx, tenant, msg)

	case strings.HasSuffix(msg.RoutingKey, ".telemetry"):
		c.processTelemetryMessage(orgCtx, tenant, msg)

	case strings.HasSuffix(msg.RoutingKey, ".event"):
		c.processEventMessage(orgCtx, tenant, msg)

	case strings.HasSuffix(msg.RoutingKey, ".location"):
		c.processLocationMessage(orgCtx, tenant, msg)

	default:
		c.logger.Warn("Unknown routing key suffix, NACKing without requeue",
			zap.String("routing_key", msg.RoutingKey),
			zap.String("org", tenant.OrgSlug))
		_ = msg.Nack(false, false)
	}
}

// processActivityLogMessage handles messages with routing keys ending in ".raw_data_log".
func (c *MultiTenantConsumer) processActivityLogMessage(orgCtx context.Context, tenant *TenantConsumer, msg amqp.Delivery) {
	var rawLog struct {
		ID              string                 `json:"id"`
		Timestamp       string                 `json:"timestamp"`
		DeviceEUI       string                 `json:"device_eui,omitempty"`
		OriginalPayload map[string]interface{} `json:"original_payload"`
		ProcessingInfo  map[string]interface{} `json:"processing_info"`
	}
	if err := json.Unmarshal(msg.Body, &rawLog); err != nil {
		c.logger.Error("Failed to unmarshal activity log",
			zap.Error(err),
			zap.String("org", tenant.OrgSlug))
		_ = msg.Nack(false, false)
		return
	}

	if rawLog.DeviceEUI == "" {
		c.logger.Warn("Activity log rejected: DeviceEUI empty",
			zap.String("org", tenant.OrgSlug),
			zap.String("routing_key", msg.RoutingKey))
		_ = msg.Nack(false, false)
		return
	}

	// Parse timestamp (RFC3339), default to now if invalid
	ts, _ := time.Parse(time.RFC3339, rawLog.Timestamp)
	if ts.IsZero() {
		ts = time.Now()
	}

	logData, err := json.Marshal(rawLog)
	if err != nil {
		c.logger.Error("Failed to marshal activity log",
			zap.Error(err),
			zap.String("org", tenant.OrgSlug))
		_ = msg.Nack(false, false)
		return
	}

	activityLog := &models.DeviceActivityLog{
		ID:        rawLog.ID,
		Time:      ts,
		DeviceEUI: rawLog.DeviceEUI,
		Payload:   logData,
	}

	if err := c.processor.ProcessActivityLog(orgCtx, tenant.OrgSlug, activityLog); err != nil {
		c.logger.Error("Failed to save activity log",
			zap.String("org", tenant.OrgSlug),
			zap.Error(err))
		_ = msg.Nack(false, true)
		return
	}

	if ackErr := msg.Ack(false); ackErr != nil {
		c.logger.Error("Failed to ack message", zap.Error(ackErr))
	}
}

// processTelemetryMessage handles messages with routing keys ending in ".telemetry".
func (c *MultiTenantConsumer) processTelemetryMessage(orgCtx context.Context, tenant *TenantConsumer, msg amqp.Delivery) {
	var telemetry models.TelemetryPayload
	if err := json.Unmarshal(msg.Body, &telemetry); err != nil {
		c.logger.Error("Failed to unmarshal telemetry payload",
			zap.Error(err),
			zap.String("org", tenant.OrgSlug))
		_ = msg.Nack(false, false)
		return
	}

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
		_ = msg.Nack(false, true)
		return
	}

	if ackErr := msg.Ack(false); ackErr != nil {
		c.logger.Error("Failed to ack message", zap.Error(ackErr))
	}
}

// processEventMessage handles messages with routing keys ending in ".event".
func (c *MultiTenantConsumer) processEventMessage(orgCtx context.Context, tenant *TenantConsumer, msg amqp.Delivery) {
	var event models.Event
	if err := json.Unmarshal(msg.Body, &event); err != nil {
		c.logger.Error("Failed to unmarshal event",
			zap.Error(err),
			zap.String("org", tenant.OrgSlug))
		_ = msg.Nack(false, false)
		return
	}

	if err := c.processor.ProcessLNSAlertEvent(orgCtx, &event); err != nil {
		c.logger.Error("Failed to process LNS event",
			zap.Error(err),
			zap.String("org", tenant.OrgSlug))
		_ = msg.Nack(false, true)
		return
	}

	if ackErr := msg.Ack(false); ackErr != nil {
		c.logger.Error("Failed to ack message", zap.Error(ackErr))
	}
}

// processLocationMessage handles messages with routing keys ending in ".location".
func (c *MultiTenantConsumer) processLocationMessage(orgCtx context.Context, tenant *TenantConsumer, msg amqp.Delivery) {
	var deviceMsg models.DeviceLocationMessage
	if err := json.Unmarshal(msg.Body, &deviceMsg); err != nil {
		c.logger.Error("Failed to unmarshal location message",
			zap.Error(err),
			zap.String("org", tenant.OrgSlug))
		_ = msg.Nack(false, false)
		return
	}

	if err := c.processor.ProcessDeviceLocation(orgCtx, &deviceMsg); err != nil {
		c.logger.Error("Failed to process location message",
			zap.Error(err),
			zap.String("org", tenant.OrgSlug))
		if errors.Is(err, timescaledb.ErrLocationDroppedTimeout) {
			c.logger.Warn("Location dropped due to timeout",
				zap.String("org", tenant.OrgSlug))
			_ = msg.Nack(false, true)
			return
		}
		return
	}

	if ackErr := msg.Ack(false); ackErr != nil {
		c.logger.Error("Failed to ack message", zap.Error(ackErr))
	}
}
