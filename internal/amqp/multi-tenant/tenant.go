package amqp

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/Space-DF/telemetry-service/internal/models"
	amqp "github.com/rabbitmq/amqp091-go"
)

// TenantConsumer represents a consumer for a specific tenant
type TenantConsumer struct {
	OrgSlug     string
	Vhost       string
	QueueName   string
	Exchange    string
	ConsumerTag string
	Channel     *amqp.Channel
	Cancel      context.CancelFunc
	ParentCtx   context.Context
}

// SchemaInitializer handles database schema initialization
type SchemaInitializer interface {
	CreateSchemaAndTables(ctx context.Context, orgSlug string) error
}

type MessageProcessor interface {
	ProcessDeviceLocation(context context.Context, msg *models.DeviceLocationMessage) error
	ProcessTelemetry(context context.Context, payload *models.TelemetryPayload) error
	ProcessLNSAlertEvent(ctx context.Context, event *models.Event) error
	ProcessActivityLog(ctx context.Context, orgSlug string, log *models.DeviceActivityLog) error
	OnOrgCreated(ctx context.Context, orgSlug string) error
	OnOrgDeleted(ctx context.Context, orgSlug string) error
}

// generateInstanceID creates a unique identifier for this service instance
func generateInstanceID() string {
	// Try to use hostname first
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}

	// Generate random bytes for uniqueness
	randomBytes := make([]byte, 4)
	if _, err := rand.Read(randomBytes); err != nil {
		// Fallback to timestamp if random fails
		return fmt.Sprintf("%s-%d", hostname, time.Now().UnixNano())
	}

	return fmt.Sprintf("%s-%s", hostname, hex.EncodeToString(randomBytes))
}

func messageKindFromRoutingKey(routingKey string) string {
	if strings.HasSuffix(routingKey, ".device.data") {
		return "skip"
	}
	switch {
	case strings.HasSuffix(routingKey, ".telemetry"):
		return "entity_telemetry"
	case strings.HasSuffix(routingKey, ".event"):
		return "event"
	case strings.HasSuffix(routingKey, ".location"):
		return "location_update"
	case strings.HasSuffix(routingKey, ".activity_log"):
		return "activity_log"
	default:
		return "skip"
	}
}

func extractOrgFromRoutingKey(routingKey string) string {
	parts := strings.Split(routingKey, ".")
	if len(parts) >= 2 && parts[0] == "tenant" {
		return parts[1]
	}
	return ""
}
