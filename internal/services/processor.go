package services

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	alertregistry "github.com/Space-DF/telemetry-service/internal/alerts/registry"
	"github.com/Space-DF/telemetry-service/internal/client"
	"github.com/Space-DF/telemetry-service/internal/events/registry"
	"github.com/Space-DF/telemetry-service/internal/models"
	notifications "github.com/Space-DF/telemetry-service/internal/services/notifications"
	timescaledb "github.com/Space-DF/telemetry-service/internal/timescaledb"
	"go.uber.org/zap"
)

// LoggingInterval is the interval for logging processing progress
const LoggingInterval = 100

// LocationProcessor processes device location messages and stores them in Psql
type LocationProcessor struct {
	tsClient      *timescaledb.Client
	ruleRegistry  *registry.RuleRegistry
	logger        *zap.Logger
	consoleClient *client.ConsoleServiceClient

	// Counters for monitoring
	processedCount atomic.Int64
	errorCount     atomic.Int64
	droppedCount   atomic.Int64

	lastAlertLevels sync.Map
}

// NewLocationProcessor creates a new location processor
func NewLocationProcessor(tsClient *timescaledb.Client, ruleRegistry *registry.RuleRegistry, logger *zap.Logger) *LocationProcessor {
	return &LocationProcessor{
		tsClient:      tsClient,
		ruleRegistry:  ruleRegistry,
		logger:        logger,
		consoleClient: client.NewConsoleServiceClient(logger),
	}
}

// ProcessTelemetryAndTriggerAutomations processes a device telemetry message and triggers any matched automation events
func (p *LocationProcessor) ProcessDeviceLocation(ctx context.Context, msg *models.DeviceLocationMessage) error {
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

	// Log progress at configured intervals
	if p.processedCount.Load()%LoggingInterval == 0 {
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
	// Legacy location messages require a space unless the device is public.
	if msg.Space == "" && !msg.IsPublished {
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

	// Match automation rules and create events for matched rules
	if p.ruleRegistry != nil {
		matchedEvents := p.ruleRegistry.MatchAutomationEvents(ctx,
			payload.DeviceID,
			payload.DeviceInfo.Manufacturer,
			payload.DeviceInfo.Model,
			payload.Entities)

		for _, event := range matchedEvents {
			// Set timestamp to current time if not set
			if event.Timestamp == 0 {
				event.Timestamp = time.Now().UnixMilli()
			}
			if err := p.tsClient.CreateAndPublishAutomationEvent(ctx, payload.Organization, &event, payload.SpaceSlug, payload.DeviceID, payload.IsPublished); err != nil {
				p.logger.Error("Failed to create event",
					zap.Error(err),
					zap.String("device_id", event.DeviceID),
					zap.String("rule_key", event.RuleKey))
			} else {
				p.logger.Info("Event created from rule match",
					zap.String("device_id", event.DeviceID),
					zap.String("rule_key", event.RuleKey),
					zap.String("event_type", event.EventType),
					zap.String("event_level", event.EventLevel))
			}
		}
	}

	if err := p.processWaterLevelAlerts(ctx, payload); err != nil {
		p.logger.Warn("Failed to process water level alerts",
			zap.Error(err),
			zap.String("device_id", payload.DeviceID))
	}

	return nil
}

func (p *LocationProcessor) processWaterLevelAlerts(ctx context.Context, payload *models.TelemetryPayload) error {
	if payload == nil || len(payload.Entities) == 0 {
		return nil
	}

	deviceModel := strings.ToLower(strings.TrimSpace(payload.DeviceInfo.Model))
	deviceModelID := strings.ToLower(strings.TrimSpace(payload.DeviceInfo.ModelID))
	isWaterLevelDevice := strings.Contains(deviceModel, "wlbv1") ||
		strings.Contains(deviceModelID, "2bbb6138-11e2-4af1-95c6-80f5fc4ec9e8") ||
		strings.Contains(deviceModel, "water level")

	if !isWaterLevelDevice {
		return nil
	}

	processor, ok := alertregistry.Get("water_depth")
	if !ok {
		p.logger.Warn("Water level processor is not registered; skipping alert processing")
		return nil
	}

	for _, entity := range payload.Entities {
		if !strings.EqualFold(entity.EntityType, "water_depth") && !strings.EqualFold(entity.EntityType, "waterlevel") {
			continue
		}

		value, err := parseFloatValue(entity.State)
		if err != nil {
			p.logger.Debug("Skipping water-level entity due to invalid value",
				zap.String("device_id", payload.DeviceID),
				zap.String("entity_id", entity.EntityID),
				zap.String("entity_type", entity.EntityType),
				zap.Any("state", entity.State),
				zap.Error(err),
			)
			continue
		}

		safeThreshold, cautionThreshold, warningThreshold := p.resolveWaterLevelThresholds(ctx, payload, processor)
		level := processor.DetermineLevel(value, safeThreshold, cautionThreshold, warningThreshold)

		p.logger.Info("Water-level threshold evaluation",
			zap.String("device_id", payload.DeviceID),
			zap.String("entity_id", entity.EntityID),
			zap.String("entity_type", entity.EntityType),
			zap.Float64("value", value),
			zap.Float64("safe_threshold", safeThreshold),
			zap.Float64("caution_threshold", cautionThreshold),
			zap.Float64("warning_threshold", warningThreshold),
			zap.String("resolved_level", level),
		)

		if level == "safe" {
			p.lastAlertLevels.Delete(alertKey(payload.Organization, payload.SpaceSlug, payload.DeviceID, entity.EntityID))
			p.logger.Debug("Water-level value below alert threshold",
				zap.String("device_id", payload.DeviceID),
				zap.String("entity_id", entity.EntityID),
				zap.String("entity_type", entity.EntityType),
				zap.Float64("value", value),
				zap.String("reason", "value below safe threshold; no notification sent"),
			)
			continue
		}

		p.logger.Info("Water-level alert triggered",
			zap.String("device_id", payload.DeviceID),
			zap.String("entity_id", entity.EntityID),
			zap.String("entity_type", entity.EntityType),
			zap.Float64("value", value),
			zap.String("level", level),
		)

		message := processor.GenerateMessage(level, value)

		alert := &notifications.Alert{
			ID:        entity.EntityID,
			Title:     message,
			DeviceID:  payload.DeviceID,
			SpaceSlug: payload.SpaceSlug,
			IsPublic:  payload.IsPublished,
			Level:     level,
			Message:   message,
			Timestamp: time.Now().UnixMilli(),
		}

		reportedAt := parseAlertReportedAt(entity.Timestamp, payload.Timestamp)
		waterDepth := fmt.Sprintf("%.2f", value)
		unit := strings.TrimSpace(processor.Unit())
		if unit == "" {
			unit = strings.TrimSpace(entity.UnitOfMeas)
		}

		brokerAlert := &models.Alert{
			Type:         "alert",
			Title:        message,
			Level:        &level,
			Organization: payload.Organization,
			SpaceSlug:    payload.SpaceSlug,
			IsPublic:     payload.IsPublished,
			DeviceID:     payload.DeviceID,
			EntityID:     &entity.EntityID,
			Message:      message,
			WaterDepth:   &waterDepth,
			Unit:         unit,
			ReportedAt:   reportedAt,
			Threshold: &models.Threshold{
				Caution:  cautionThreshold,
				Critical: warningThreshold,
				Warning:  warningThreshold,
			},
		}

		if err := p.tsClient.PublishAlertToDevice(ctx, brokerAlert, payload.Organization); err != nil {
			p.logger.Warn("Failed to publish water-level alert to broker",
				zap.String("device_id", payload.DeviceID),
				zap.String("entity_id", entity.EntityID),
				zap.String("level", level),
				zap.Error(err),
			)
		} else {
			p.logger.Debug("Water-level alert published to broker",
				zap.String("device_id", payload.DeviceID),
				zap.String("entity_id", entity.EntityID),
				zap.String("level", level),
			)
		}

		p.logger.Info("[ALERT->NOTIFICATION] sending alert notification payload",
			zap.String("device_id", payload.DeviceID),
			zap.String("org", payload.Organization),
			zap.String("space_slug", payload.SpaceSlug),
			zap.String("title", alert.Title),
			zap.String("message", alert.Message),
			zap.String("level", alert.Level),
		)

		if err := p.tsClient.NotifyAlert(ctx, alert, payload.Organization); err != nil {
			p.logger.Warn("Failed to send water level alert notification",
				zap.String("device_id", payload.DeviceID),
				zap.String("entity_id", entity.EntityID),
				zap.String("level", level),
				zap.Error(err),
			)
		}

	}

	return nil
}

func parseAlertReportedAt(entityTimestamp, payloadTimestamp string) time.Time {
	reportedAt := parseTimestamp(entityTimestamp)
	if reportedAt.IsZero() {
		reportedAt = parseTimestamp(payloadTimestamp)
	}
	if reportedAt.IsZero() {
		reportedAt = time.Now().UTC()
	}
	return reportedAt
}

func parseTimestamp(value string) time.Time {
	if strings.TrimSpace(value) == "" {
		return time.Time{}
	}
	reportedAt, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return time.Time{}
	}
	return reportedAt.UTC()
}

func (p *LocationProcessor) resolveWaterLevelThresholds(ctx context.Context, payload *models.TelemetryPayload, processor alertregistry.Processor) (float64, float64, float64) {
	safeThreshold := processor.DefaultSafeThreshold()
	cautionThreshold := processor.DefaultCautionThreshold()
	warningThreshold := processor.DefaultWarningThreshold()

	if p.consoleClient == nil {
		return safeThreshold, cautionThreshold, warningThreshold
	}

	monitoring, err := p.consoleClient.GetOrganizationMonitoring(ctx, payload.Organization)
	if err != nil {
		p.logger.Warn("Failed to load monitoring thresholds from console service, falling back to defaults",
			zap.String("org", payload.Organization),
			zap.Error(err),
		)
		return safeThreshold, cautionThreshold, warningThreshold
	}

	safeThreshold, cautionThreshold, warningThreshold = client.ResolveThresholds(safeThreshold, cautionThreshold, warningThreshold, monitoring)
	p.logger.Debug("Resolved water-level thresholds from console service",
		zap.String("org", payload.Organization),
		zap.Float64("safe_threshold", safeThreshold),
		zap.Float64("caution_threshold", cautionThreshold),
		zap.Float64("warning_threshold", warningThreshold),
	)

	return safeThreshold, cautionThreshold, warningThreshold
}

func parseFloatValue(state any) (float64, error) {
	switch v := state.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case string:
		return strconv.ParseFloat(v, 64)
	default:
		return 0, fmt.Errorf("unsupported type %T", v)
	}
}

func alertKey(org, spaceSlug, deviceID, entityID string) string {
	return fmt.Sprintf("%s:%s:%s:%s", strings.TrimSpace(org), strings.TrimSpace(spaceSlug), strings.TrimSpace(deviceID), strings.TrimSpace(entityID))
}

func (p *LocationProcessor) ProcessActivityLog(ctx context.Context, orgSlug string, log *models.DeviceActivityLog) error {
	if log == nil {
		return nil
	}
	return p.tsClient.InsertActivityLog(ctx, orgSlug, *log)
}

func (p *LocationProcessor) ProcessLNSAlertEvent(ctx context.Context, event *models.Event) error {
	if event == nil || event.EventType == "" {
		return nil
	}

	if event.DeviceID == "" || event.SpaceSlug == "" {
		return fmt.Errorf("device_id and space_slug are required")
	}

	if event.TimeFiredTs == 0 {
		event.TimeFiredTs = time.Now().UnixMilli()
	}

	if p.tsClient == nil {
		return fmt.Errorf("timescaledb client is not initialized")
	}

	org := timescaledb.OrgFromContext(ctx)
	if org == "" {
		org = event.Organization
	}

	return p.tsClient.CreateLNSAlertEvent(ctx, org, event)
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
