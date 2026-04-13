package amqp

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Space-DF/telemetry-service/internal/circuitbreaker"
	"github.com/Space-DF/telemetry-service/internal/config"
	"github.com/Space-DF/telemetry-service/internal/models"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
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

// MultiTenantConsumer handles message consumption for multiple tenants
type MultiTenantConsumer struct {
	config           config.AMQP
	orgEventsConfig  config.OrgEvents
	orgEventsConn    *amqp.Connection
	orgEventsChannel *amqp.Channel
	processor        MessageProcessor
	schemaInit       SchemaInitializer
	logger           *zap.Logger
	done             chan bool
	instanceID       string // Unique identifier for this instance

	tenantMu        sync.RWMutex
	tenantConsumers map[string]*TenantConsumer

	vhostMu          sync.Mutex
	vhostConnections map[string]*pooledConnection

	// Connection monitoring and circuit breaker
	circuitBreaker       *circuitbreaker.CircuitBreaker
	reconnectChan        chan struct{}
	connCloseNotifier    chan *amqp.Error
	channelCloseNotifier chan *amqp.Error
	reconnecting         atomic.Bool
	orgEventsCancel      context.CancelFunc
	monitorCancel        context.CancelFunc
	monitorWg            sync.WaitGroup
	stopOnce             sync.Once
}

// MessageProcessor processes device location messages
type MessageProcessor interface {
	ProcessMessage(context context.Context, msg *models.DeviceLocationMessage) error
	ProcessTelemetry(ctx context.Context, payload *models.TelemetryPayload) error
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

// NewMultiTenantConsumer creates a new multi-tenant consumer
func NewMultiTenantConsumer(cfg config.AMQP, orgEventsCfg config.OrgEvents, processor MessageProcessor, schemaInit SchemaInitializer, logger *zap.Logger) *MultiTenantConsumer {
	instanceID := generateInstanceID()

	logger.Info("Creating multi-tenant consumer with unique instance ID",
		zap.String("instance_id", instanceID))

	// Create circuit breaker for connection attempts
	cbConfig := circuitbreaker.Config{
		MaxFailures:      5,
		ResetTimeout:     30 * time.Second,
		SuccessThreshold: 2,
	}
	cb := circuitbreaker.New(cbConfig)

	return &MultiTenantConsumer{
		config:               cfg,
		orgEventsConfig:      orgEventsCfg,
		processor:            processor,
		schemaInit:           schemaInit,
		logger:               logger,
		done:                 make(chan bool, 1),
		instanceID:           instanceID,
		tenantConsumers:      make(map[string]*TenantConsumer),
		vhostConnections:     make(map[string]*pooledConnection),
		circuitBreaker:       cb,
		reconnectChan:        make(chan struct{}, 1),
		connCloseNotifier:    make(chan *amqp.Error, 1),
		channelCloseNotifier: make(chan *amqp.Error, 1),
	}
}

func (c *MultiTenantConsumer) makeConsumerTag(orgSlug, vhost string) string {
	safeVhost := strings.NewReplacer("/", "_", ".", "_", ":", "_").Replace(vhost)
	return fmt.Sprintf("telemetry-%s-%s-%s", orgSlug, safeVhost, c.instanceID)
}

// subscribeToOrganization starts consuming from an organization's queue
func (c *MultiTenantConsumer) subscribeToOrganization(parentCtx context.Context, orgSlug, vhost, queueName, exchange string) error {
	// Create default queue and exchange names if not provided
	if queueName == "" {
		queueName = fmt.Sprintf("%s.telemetry.queue", orgSlug)
	}
	if exchange == "" {
		exchange = fmt.Sprintf("%s.exchange", orgSlug)
	}

	if !c.shouldHandleVhost(vhost) {
		c.logger.Info("Skipping subscription; vhost not assigned to this telemetry service",
			zap.String("org", orgSlug),
			zap.String("vhost", vhost))
		return nil
	}

	c.tenantMu.RLock()
	if _, exists := c.tenantConsumers[orgSlug]; exists {
		c.tenantMu.RUnlock()
		c.logger.Info("Subscription already active",
			zap.String("org", orgSlug),
			zap.String("vhost", vhost))
		return nil
	}
	c.tenantMu.RUnlock()

	conn, err := c.getOrCreateVhostConnection(vhost)
	if err != nil {
		return err
	}

	channel, err := conn.Channel()
	if err != nil {
		c.releaseVhostConnection(vhost)
		return fmt.Errorf("failed to open channel for vhost %s: %w", vhost, err)
	}

	if err := channel.Qos(c.config.PrefetchCount, 0, false); err != nil {
		_ = channel.Close()
		c.releaseVhostConnection(vhost)
		return fmt.Errorf("failed to set QoS for vhost %s: %w", vhost, err)
	}

	// Declare the queue - will be idempotent if it already exists with same params
	// The queue is shared across all telemetry instances for load balancing
	queue, err := channel.QueueDeclare(
		queueName,
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		_ = channel.Close()
		c.releaseVhostConnection(vhost)
		return fmt.Errorf("failed to declare queue '%s' in vhost '%s': %w", queueName, vhost, err)
	}

	// Bind queue to exchange with the routing key pattern
	routingKey := fmt.Sprintf("tenant.%s.transformed.telemetry.device.location", orgSlug)
	if err := channel.QueueBind(
		queue.Name,
		routingKey,
		exchange,
		false,
		nil,
	); err != nil {
		_ = channel.Close()
		c.releaseVhostConnection(vhost)
		return fmt.Errorf("failed to bind queue '%s' to exchange '%s': %w", queue.Name, exchange, err)
	}

	consumerTag := c.makeConsumerTag(orgSlug, vhost)

	messages, err := channel.Consume(
		queue.Name,
		consumerTag,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		_ = channel.Close()
		c.releaseVhostConnection(vhost)
		return fmt.Errorf("failed to start consuming from queue '%s' in vhost '%s': %w", queueName, vhost, err)
	}

	tenantCtx, cancel := context.WithCancel(parentCtx) //#nosec G118
	consumer := &TenantConsumer{
		OrgSlug:     orgSlug,
		Vhost:       vhost,
		QueueName:   queueName,
		Exchange:    exchange,
		ConsumerTag: consumerTag,
		Channel:     channel,
		Cancel:      cancel,
		ParentCtx:   parentCtx,
	}

	c.tenantMu.Lock()
	c.tenantConsumers[orgSlug] = consumer
	c.tenantMu.Unlock()

	go c.processTenantMessages(tenantCtx, consumer, messages)

	c.logger.Info("Started consuming from organization",
		zap.String("org", orgSlug),
		zap.String("vhost", vhost),
		zap.String("queue", queueName),
		zap.String("routing_key", routingKey))
	return nil
}

// unsubscribeFromOrganization stops consuming from an organization's queue
func (c *MultiTenantConsumer) unsubscribeFromOrganization(orgSlug string) {
	c.tenantMu.Lock()
	consumer, exists := c.tenantConsumers[orgSlug]
	if exists {
		delete(c.tenantConsumers, orgSlug)
	}
	c.tenantMu.Unlock()

	if !exists || consumer == nil {
		return
	}

	consumer.Cancel() // Cancel the context to stop the goroutine

	if consumer.Channel != nil {
		_ = consumer.Channel.Cancel(consumer.ConsumerTag, false)
		_ = consumer.Channel.Close()
	}

	c.releaseVhostConnection(consumer.Vhost)

	c.logger.Info("Stopped consuming from organization",
		zap.String("org", consumer.OrgSlug),
		zap.String("vhost", consumer.Vhost),
		zap.String("queue", consumer.QueueName))
}

// stopAllConsumers stops all active tenant consumers
func (c *MultiTenantConsumer) stopAllConsumers() {
	c.logger.Info("Stopping all tenant consumers")
	c.tenantMu.Lock()
	for slug, consumer := range c.tenantConsumers {
		if consumer != nil {
			consumer.Cancel()
			if consumer.Channel != nil {
				_ = consumer.Channel.Cancel(consumer.ConsumerTag, false)
				_ = consumer.Channel.Close()
			}
			c.releaseVhostConnection(consumer.Vhost)
			c.logger.Info("Stopped consuming from organization",
				zap.String("org", consumer.OrgSlug),
				zap.String("vhost", consumer.Vhost))
		}
		delete(c.tenantConsumers, slug)
	}
	c.tenantMu.Unlock()

	c.vhostMu.Lock()
	for vhost, pooled := range c.vhostConnections {
		if pooled != nil && pooled.conn != nil {
			_ = pooled.conn.Close()
		}
		delete(c.vhostConnections, vhost)
	}
	c.vhostMu.Unlock()
	c.logger.Info("All tenant consumers stopped")
}

// resubscribeTenant resubscribes to a tenant's queue after a channel closure
func (c *MultiTenantConsumer) resubscribeTenant(_ context.Context, oldTenant *TenantConsumer) {
	// Check if main connection is down - trigger reconnection if needed
	if c.orgEventsConn == nil || c.orgEventsConn.IsClosed() {
		c.logger.Info("Main connection down, triggering centralized reconnection",
			zap.String("org", oldTenant.OrgSlug))
		if !c.reconnecting.Load() {
			select {
			case c.reconnectChan <- struct{}{}:
			default:
			}
		}
		return
	}

	// Cancel the old consumer goroutine
	oldTenant.Cancel()
	if oldTenant.Channel != nil {
		_ = oldTenant.Channel.Cancel(oldTenant.ConsumerTag, false)
		_ = oldTenant.Channel.Close()
	}
	// Release vhost connection to properly decrement refCount
	c.releaseVhostConnection(oldTenant.Vhost)

	backoff := 1 * time.Second
	maxBackoff := 30 * time.Second
	maxAttempts := 5

	for attempt := 0; attempt < maxAttempts; attempt++ {
		select {
		case <-oldTenant.ParentCtx.Done():
			return
		case <-time.After(backoff):
		}

		// Check main connection again before each attempt
		if c.orgEventsConn == nil || c.orgEventsConn.IsClosed() {
			c.logger.Info("Main connection went down, aborting individual resubscription",
				zap.String("org", oldTenant.OrgSlug))
			return
		}

		// Remove from map first so subscribeToOrganization can create a new subscription
		c.tenantMu.Lock()
		delete(c.tenantConsumers, oldTenant.OrgSlug)
		c.tenantMu.Unlock()

		// Try to resubscribe using parent context (not tenant's cancelled context)
		err := c.subscribeToOrganization(oldTenant.ParentCtx, oldTenant.OrgSlug, oldTenant.Vhost, oldTenant.QueueName, oldTenant.Exchange)
		if err == nil {
			c.logger.Info("Successfully resubscribed tenant",
				zap.String("org", oldTenant.OrgSlug),
				zap.Int("attempt", attempt+1))
			return
		}

		c.logger.Warn("Failed to resubscribe to tenant",
			zap.String("org", oldTenant.OrgSlug),
			zap.Int("attempt", attempt+1),
			zap.Error(err))

		// Exponential backoff
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}

	// If all attempts failed, remove from map so main reconnection can handle it
	c.tenantMu.Lock()
	delete(c.tenantConsumers, oldTenant.OrgSlug)
	c.tenantMu.Unlock()

	c.logger.Warn("Failed to resubscribe tenant after all attempts, removed from map for main reconnection",
		zap.String("org", oldTenant.OrgSlug))
}
