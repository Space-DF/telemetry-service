package services

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/Space-DF/telemetry-service/internal/models"
	"github.com/Space-DF/telemetry-service/internal/timescaledb"
	"go.uber.org/zap"
)

// LocationProcessor processes device location messages and stores them in Psql
type LocationProcessor struct {
	tsClient *timescaledb.Client
	logger   *zap.Logger

	// Counters for monitoring
	processedCount atomic.Int64
	errorCount     atomic.Int64
	droppedCount   atomic.Int64
}

// NewLocationProcessor creates a new location processor
func NewLocationProcessor(tsClient *timescaledb.Client, logger *zap.Logger) *LocationProcessor {
	return &LocationProcessor{
		tsClient: tsClient,
		logger:   logger,
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

	// Convert to location record
	location := msg.ToLocationSetter()
	if location == nil {
		p.logger.Debug("No location generated from message (unknown device or no coordinates)",
			zap.String("device_id", msg.DeviceID),
		)
		p.droppedCount.Add(1)
		return nil
	}

	// Add location to batch
	if err := p.tsClient.AddLocation(ctx, location); err != nil {
		p.errorCount.Add(1)
		return fmt.Errorf("failed to add location to batch: %w", err)
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
