package services

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Space-DF/telemetry-service/internal/events/registry"
	"github.com/Space-DF/telemetry-service/internal/models"
	timescaledb "github.com/Space-DF/telemetry-service/internal/timescaledb"
	"go.uber.org/zap"
)

// LocationProcessor processes device location messages and stores them in Psql
type LocationProcessor struct {
	tsClient     *timescaledb.Client
	ruleRegistry *registry.RuleRegistry
	logger       *zap.Logger

	// Counters for monitoring
	processedCount atomic.Int64
	errorCount     atomic.Int64
	droppedCount   atomic.Int64
}

// NewLocationProcessor creates a new location processor
func NewLocationProcessor(tsClient *timescaledb.Client, ruleRegistry *registry.RuleRegistry, logger *zap.Logger) *LocationProcessor {
	return &LocationProcessor{
		tsClient:     tsClient,
		ruleRegistry: ruleRegistry,
		logger:       logger,
	}
}

// ProcessMessage processes a device location message
func (p *LocationProcessor) ProcessMessage(ctx context.Context, msg *models.DeviceLocationMessage) error {
	p.logger.Debug("Processing device location message",
		zap.String("device_id", msg.DeviceID),
		zap.String("space", msg.Space),
		zap.Float64("latitude", msg.Location.Latitude),
		zap.Float64("longitude", msg.Location.Longitude),
	)

	// Validate message
	if err := p.validateMessage(msg); err != nil {
		p.logger.Warn("Invalid message, dropping",
			zap.Error(err),
		)
		p.droppedCount.Add(1)
		return nil // Don't retry invalid messages
	}

	// Skip if we don't have valid coordinates
	if msg.Location.Latitude == 0 && msg.Location.Longitude == 0 {
		p.logger.Debug("Skipping message with no coordinates",
			zap.String("device_id", msg.DeviceID),
		)
		p.droppedCount.Add(1)
		return nil
	}

	// Convert to telemetry payload and save to entity_states
	payload := msg.ToTelemetryPayload()
	if payload == nil {
		p.logger.Debug("No payload generated from message (unknown device or no coordinates)",
			zap.String("device_id", msg.DeviceID),
		)
		p.droppedCount.Add(1)
		return nil
	}

	// Save telemetry payload to entity_states
	if err := p.tsClient.SaveTelemetryPayload(ctx, payload); err != nil {
		p.errorCount.Add(1)
		p.logger.Error("Failed to save telemetry payload",
			zap.Error(err),
			zap.String("device_id", msg.DeviceID),
		)
		return fmt.Errorf("failed to save telemetry payload: %w", err)
	}

	p.processedCount.Add(1)

	p.logger.Debug("Successfully processed message",
		zap.String("device_id", msg.DeviceID),
		zap.Int64("total_processed", p.processedCount.Load()),
	)

	// Log progress every 100 messages
	if p.processedCount.Load()%100 == 0 {
		p.logger.Info("Processing progress",
			zap.Int64("processed", p.processedCount.Load()),
			zap.Int64("errors", p.errorCount.Load()),
			zap.Int64("dropped", p.droppedCount.Load()),
		)
	}

	return nil
}

// validateMessage validates a device location message
func (p *LocationProcessor) validateMessage(msg *models.DeviceLocationMessage) error {
	// Check required fields
	if msg.Space == "" {
		return fmt.Errorf("missing space")
	}

	if msg.Timestamp == "" {
		return fmt.Errorf("missing timestamp")
	}

	// Validate location data if present
	if msg.Location.Latitude == 0 && msg.Location.Longitude == 0 {
		// Location is optional, but if one coordinate is set, both should be
		if msg.Location.Latitude != 0 || msg.Location.Longitude != 0 {
			return fmt.Errorf("incomplete location data")
		}
	} else {
		// Validate coordinate ranges
		if msg.Location.Latitude < -90 || msg.Location.Latitude > 90 {
			return fmt.Errorf("invalid latitude: %f", msg.Location.Latitude)
		}

		if msg.Location.Longitude < -180 || msg.Location.Longitude > 180 {
			return fmt.Errorf("invalid longitude: %f", msg.Location.Longitude)
		}
	}

	return nil
}

// ProcessTelemetry processes the entity-centric telemetry payload and stores it in the entities schema.
func (p *LocationProcessor) ProcessTelemetry(ctx context.Context, payload *models.TelemetryPayload) error {
	if payload == nil {
		return fmt.Errorf("nil telemetry payload")
	}

	if p.tsClient == nil {
		return fmt.Errorf("timescaledb client is not initialized")
	}

	p.logger.Info("Processing telemetry payload",
		zap.String("org", payload.Organization),
		zap.String("device_id", payload.DeviceID),
		zap.Int("entities", len(payload.Entities)),
	)

	if err := p.tsClient.SaveTelemetryPayload(ctx, payload); err != nil {
		p.logger.Error("Failed to persist telemetry payload", zap.Error(err))
		return err
	}

	// Evaluate rules and create events for matched rules
	if p.ruleRegistry != nil {
		matchedEvents := p.ruleRegistry.Evaluate(ctx,
			payload.DeviceID,
			payload.DeviceInfo.Manufacturer,
			payload.DeviceInfo.Model,
			payload.Entities)

		for _, event := range matchedEvents {
			// Set timestamp to current time if not set
			if event.Timestamp == 0 {
				event.Timestamp = time.Now().UnixMilli()
			}
			if err := p.tsClient.CreateEvent(ctx, payload.Organization, &event, payload.SpaceSlug); err != nil {
				p.logger.Error("Failed to create event",
					zap.Error(err),
					zap.String("entity_id", event.EntityID),
					zap.String("rule_key", event.RuleKey))
			} else {
				p.logger.Info("Event created from rule match",
					zap.String("entity_id", event.EntityID),
					zap.String("rule_key", event.RuleKey),
					zap.String("event_type", event.EventType),
					zap.String("rule_source", event.RuleSource),
					zap.String("event_level", event.EventLevel),
					zap.Float64("value", event.Value),
					zap.Float64("threshold", event.Threshold))
			}
		}
	}

	return nil
}

// GetStats returns processor statistics
func (p *LocationProcessor) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"processed_count": p.processedCount.Load(),
		"error_count":     p.errorCount.Load(),
		"dropped_count":   p.droppedCount.Load(),
	}
}

// OnOrgCreated is invoked when a new organization is created. It ensures any
// organization-specific setup is performed, such as creating a dedicated DB schema.
func (p *LocationProcessor) OnOrgCreated(ctx context.Context, orgSlug string) error {
	p.logger.Info("Handling org created in processor", zap.String("org", orgSlug))

	if orgSlug == "" {
		return fmt.Errorf("empty org slug")
	}

	if p.tsClient == nil {
		return fmt.Errorf("timescaledb client is not initialized")
	}

	if err := p.tsClient.CreateSchemaAndTables(ctx, orgSlug); err != nil {
		p.logger.Error("Failed to create org schema/tables", zap.String("org", orgSlug), zap.Error(err))
		return err
	}

	p.logger.Info("Organization schema created/ensured", zap.String("org", orgSlug))
	return nil
}

// OnOrgDeleted is invoked when an organization is deleted. It performs
// cleanup for organization-specific resources such as dropping the DB schema.
func (p *LocationProcessor) OnOrgDeleted(ctx context.Context, orgSlug string) error {
	p.logger.Info("Handling org deleted in processor", zap.String("org", orgSlug))

	if orgSlug == "" {
		return fmt.Errorf("empty org slug")
	}

	if p.tsClient == nil {
		return fmt.Errorf("timescaledb client is not initialized")
	}

	// Attempt to drop the schema. This is destructive and should only be
	// called after ensuring no active processing remains for the org.
	if err := p.tsClient.DropSchema(ctx, orgSlug); err != nil {
		p.logger.Error("Failed to drop org schema", zap.String("org", orgSlug), zap.Error(err))
		return err
	}

	p.logger.Info("Organization schema dropped", zap.String("org", orgSlug))
	return nil
}
