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
		zap.String("device_eui", msg.DeviceEUI),
		zap.String("device_id", msg.DeviceID),
		zap.String("organization", msg.Organization),
		zap.Float64("latitude", msg.Location.Latitude),
		zap.Float64("longitude", msg.Location.Longitude),
		zap.String("accuracy", msg.Location.Accuracy),
	)

	// Validate message
	if err := p.validateMessage(msg); err != nil {
		p.logger.Warn("Invalid message, dropping",
			zap.Error(err),
			zap.String("device_eui", msg.DeviceEUI),
		)
		p.droppedCount.Add(1)
		return nil // Don't retry invalid messages
	}

	// Convert to location record
	location := msg.ToLocation()
	if location == nil {
		p.logger.Debug("No location generated from message (unknown device or no coordinates)",
			zap.String("device_eui", msg.DeviceEUI),
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
		zap.String("device_eui", msg.DeviceEUI),
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
	if msg.DeviceEUI == "" {
		return fmt.Errorf("missing device_eui")
	}

	if msg.Organization == "" {
		return fmt.Errorf("missing organization")
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
