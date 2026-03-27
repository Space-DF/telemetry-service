package amqp

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Space-DF/telemetry-service/internal/circuitbreaker"
	"github.com/Space-DF/telemetry-service/internal/config"
	"github.com/Space-DF/telemetry-service/internal/models"
	"github.com/Space-DF/telemetry-service/internal/timescaledb"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

// MessageProcessor processes device location messages
type MessageProcessor interface {
	ProcessMessage(context context.Context, msg *models.DeviceLocationMessage) error
	ProcessTelemetry(ctx context.Context, payload *models.TelemetryPayload) error
	OnOrgCreated(ctx context.Context, orgSlug string) error
	OnOrgDeleted(ctx context.Context, orgSlug string) error
}

// TenantConsumer represents a consumer for a specific tenant
type TenantConsumer struct {
	OrgSlug     string
	Vhost       string
	QueueName   string
	Exchange    string
	ConsumerTag string
	Channel     *amqp.Channel
	Cancel      context.CancelFunc
}

type pooledConnection struct {
	conn     *amqp.Connection
	refCount int
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
	reconnecting         bool // Flag to prevent concurrent reconnections
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

// Connect establishes connection to AMQP broker
func (c *MultiTenantConsumer) Connect() error {
	var err error

	// Use circuit breaker for connection attempts
	err = c.circuitBreaker.Execute(func() error {
		// Connect to AMQP broker for org events
		c.orgEventsConn, err = amqp.Dial(c.config.BrokerURL)
		if err != nil {
			return fmt.Errorf("failed to connect to AMQP broker: %w", err)
		}

		// Create separate channel for org events
		c.orgEventsChannel, err = c.orgEventsConn.Channel()
		if err != nil {
			defer func() {
				if err := c.orgEventsConn.Close(); err != nil {
					c.logger.Error("Failed to close AMQP connection", zap.Error(err))
				}

			}()
			return fmt.Errorf("failed to open org events channel: %w", err)
		}

		return nil
	})

	if err != nil {
		return err
	}

	// Register close notifiers for connection monitoring
	c.setupConnectionMonitoring()

	c.logger.Info("Successfully connected to AMQP broker",
		zap.String("broker", c.config.BrokerURL))

	return nil
}

// setupConnectionMonitoring sets up close notifiers for the connection and channel
func (c *MultiTenantConsumer) setupConnectionMonitoring() {
	// Register connection close notifier
	c.connCloseNotifier = make(chan *amqp.Error, 1)
	c.orgEventsConn.NotifyClose(c.connCloseNotifier)

	// Register channel close notifier
	c.channelCloseNotifier = make(chan *amqp.Error, 1)
	c.orgEventsChannel.NotifyClose(c.channelCloseNotifier)

	// Start monitoring goroutine
	go c.monitorConnection()
}

// monitorConnection monitors the connection and channel for unexpected closures
func (c *MultiTenantConsumer) monitorConnection() {
	for {
		select {
		case <-c.done:
			return

		case err, ok := <-c.connCloseNotifier:
			if !ok {
				// Channel closed, expected during shutdown
				return
			}
			c.handleConnectionClosed(err)

		case err, ok := <-c.channelCloseNotifier:
			if !ok {
				return
			}
			c.handleChannelClosed(err)
		}
	}
}

// handleConnectionClosed handles unexpected connection closure
func (c *MultiTenantConsumer) handleConnectionClosed(err *amqp.Error) {
	c.logger.Error("AMQP connection closed unexpectedly",
		zap.Error(err),
		zap.Int("code", err.Code))

	// Invalidate all pooled vhost connections since they're also closed
	c.vhostMu.Lock()
	c.vhostConnections = make(map[string]*pooledConnection)
	c.vhostMu.Unlock()

	// Record failure in circuit breaker
	c.circuitBreaker.RecordFailure()

	// Notify reconnection goroutine
	select {
	case c.reconnectChan <- struct{}{}:
	default:
	}
}

// handleChannelClosed handles unexpected channel closure
func (c *MultiTenantConsumer) handleChannelClosed(err *amqp.Error) {
	c.logger.Error("AMQP channel closed unexpectedly",
		zap.Error(err),
		zap.Int("code", err.Code))

	// Just trigger full reconnection - it's safer and simpler
	select {
	case c.reconnectChan <- struct{}{}:
	default:
	}
}

// getOrgEventsQueueName returns a unique queue name for this instance
func (c *MultiTenantConsumer) getOrgEventsQueueName() string {
	// Each instance gets its own queue to ensure all instances receive all org events
	return fmt.Sprintf("%s.%s", c.orgEventsConfig.Queue, c.instanceID)
}

// Start begins consuming messages with multi-tenant support
func (c *MultiTenantConsumer) Start(ctx context.Context) error {
	c.logger.Info("Starting telemetry service with multi-tenant architecture",
		zap.String("instance_id", c.instanceID),
		zap.String("org_events_queue", c.getOrgEventsQueueName()))
	c.logger.Info("Waiting for organization events to discover active tenants")

	// Start reconnection monitor goroutine
	go c.reconnectionMonitor(ctx)

	// Start listening to organization events
	go func() {
		if err := c.listenToOrgEvents(ctx); err != nil {
			c.logger.Error("Org events listener error", zap.Error(err))
		}
	}()

	// Send bootstrap discovery request after a small delay
	go func() {
		time.Sleep(2 * time.Second)
		if err := c.sendDiscoveryRequest(ctx); err != nil {
			c.logger.Error("Failed to send discovery request", zap.Error(err))
		}
	}()

	// Wait for context cancellation or done signal
	select {
	case <-ctx.Done():
		c.logger.Info("Context cancelled, stopping multi-tenant consumer")
		c.stopAllConsumers()
	case <-c.done:
		c.logger.Info("Multi-tenant consumer stopped")
		c.stopAllConsumers()
	}

	return nil
}

// reconnectionMonitor monitors for reconnection requests
func (c *MultiTenantConsumer) reconnectionMonitor(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.done:
			return
		case <-c.reconnectChan:
			// Check if already reconnecting to prevent concurrent reconnections
			if c.reconnecting {
				c.logger.Debug("Already reconnecting, skipping duplicate request")
				continue
			}

			c.logger.Warn("Reconnection triggered, attempting to reconnect...")
			if err := c.reconnectConnection(ctx); err != nil {
				c.reconnecting = false
				c.logger.Error("Failed to reconnect", zap.Error(err))
				// Schedule another reconnection attempt
				go func() {
					time.Sleep(10 * time.Second)
					select {
					case c.reconnectChan <- struct{}{}:
					default:
					}
				}()
			} else {
				c.reconnecting = false
				// Restart org events listener after successful reconnection
				go func() {
					if err := c.listenToOrgEvents(ctx); err != nil {
						c.logger.Error("Org events listener error after reconnection", zap.Error(err))
					}
				}()
			}
		}
	}
}

// sendDiscoveryRequest sends a request to console service to get all active orgs
func (c *MultiTenantConsumer) sendDiscoveryRequest(ctx context.Context) error {
	c.logger.Info("Sending discovery request to console service for existing organizations",
		zap.String("reply_to", c.getOrgEventsQueueName()))

	request := models.OrgDiscoveryRequest{
		EventType:   models.OrgDiscoveryReq,
		EventID:     fmt.Sprintf("discovery-%s-%d", c.instanceID, time.Now().Unix()),
		Timestamp:   time.Now(),
		ServiceName: "telemetry-service",
		ReplyTo:     c.getOrgEventsQueueName(), // Use instance-specific queue
	}

	body, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("failed to marshal discovery request: %w", err)
	}

	err = c.orgEventsChannel.PublishWithContext(
		ctx,
		c.orgEventsConfig.Exchange,
		"org.discovery.request",
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
			Timestamp:   time.Now(),
		},
	)

	if err != nil {
		return fmt.Errorf("failed to publish discovery request: %w", err)
	}

	c.logger.Info("Discovery request sent successfully")
	return nil
}

func (c *MultiTenantConsumer) ensureOrgEventsTopology() error {
	if err := c.orgEventsChannel.ExchangeDeclare(
		c.orgEventsConfig.Exchange,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("exchange declare failed: %w", err)
	}

	// Use instance-specific queue name
	queueName := c.getOrgEventsQueueName()

	// Queue should auto-delete when this instance disconnects
	// Set exclusive to true so each instance gets its own queue
	if _, err := c.orgEventsChannel.QueueDeclare(
		queueName,
		false, // non-durable (will be recreated on restart)
		true,  // auto-delete when unused
		true,  // exclusive to this connection
		false, // no-wait
		nil,   // arguments
	); err != nil {
		return fmt.Errorf("queue declare failed for %s: %w", queueName, err)
	}

	if err := c.orgEventsChannel.QueueBind(
		queueName,
		c.orgEventsConfig.RoutingKey,
		c.orgEventsConfig.Exchange,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("queue bind failed for %s: %w", queueName, err)
	}

	c.logger.Info("Org events topology configured",
		zap.String("queue", queueName),
		zap.String("exchange", c.orgEventsConfig.Exchange),
		zap.String("routing_key", c.orgEventsConfig.RoutingKey))

	return nil
}

func (c *MultiTenantConsumer) shouldHandleVhost(vhost string) bool {
	if len(c.config.AllowedVHosts) == 0 {
		return true
	}

	for _, allowed := range c.config.AllowedVHosts {
		if allowed == vhost {
			return true
		}
	}

	return false
}

func (c *MultiTenantConsumer) buildVhostURL(vhost string) (string, error) {
	baseURL := c.config.BrokerURL
	parsed, err := url.Parse(baseURL)
	if err != nil {
		return "", fmt.Errorf("failed to parse broker url: %w", err)
	}

	if vhost == "" {
		parsed.Path = "/"
		parsed.RawPath = ""
	} else {
		encoded := "/" + url.PathEscape(vhost)
		parsed.Path = encoded
		parsed.RawPath = encoded
	}

	return parsed.String(), nil
}

func (c *MultiTenantConsumer) getOrCreateVhostConnection(vhost string) (*amqp.Connection, error) {
	c.vhostMu.Lock()
	defer c.vhostMu.Unlock()

	// Check if we have a pooled connection
	if pooled, exists := c.vhostConnections[vhost]; exists {
		// Check if connection is still valid
		if pooled.conn != nil && !pooled.conn.IsClosed() {
			pooled.refCount++
			return pooled.conn, nil
		}
		// Connection is closed, remove it and create a new one
		delete(c.vhostConnections, vhost)
	}

	// Create a new connection
	vhostURL, err := c.buildVhostURL(vhost)
	if err != nil {
		return nil, err
	}

	conn, err := amqp.Dial(vhostURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to vhost %s: %w", vhost, err)
	}

	c.vhostConnections[vhost] = &pooledConnection{
		conn:     conn,
		refCount: 1,
	}

	return conn, nil
}

func (c *MultiTenantConsumer) releaseVhostConnection(vhost string) {
	c.vhostMu.Lock()
	defer c.vhostMu.Unlock()

	pooled, exists := c.vhostConnections[vhost]
	if !exists {
		return
	}

	pooled.refCount--
	if pooled.refCount <= 0 {
		_ = pooled.conn.Close()
		delete(c.vhostConnections, vhost)
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
func (c *MultiTenantConsumer) resubscribeTenant(ctx context.Context, oldTenant *TenantConsumer) {
	// The main reconnection will handle all tenants together
	if c.orgEventsConn == nil || c.orgEventsConn.IsClosed() {
		c.logger.Info("Main connection down, waiting for centralized reconnection",
			zap.String("org", oldTenant.OrgSlug))
		return
	}

	// Cancel the old consumer goroutine
	oldTenant.Cancel()
	if oldTenant.Channel != nil {
		_ = oldTenant.Channel.Cancel(oldTenant.ConsumerTag, false)
		_ = oldTenant.Channel.Close()
	}

	backoff := 1 * time.Second
	maxBackoff := 30 * time.Second
	maxAttempts := 5

	for attempt := 0; attempt < maxAttempts; attempt++ {
		select {
		case <-ctx.Done():
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

		// Try to resubscribe
		err := c.subscribeToOrganization(ctx, oldTenant.OrgSlug, oldTenant.Vhost, oldTenant.QueueName, oldTenant.Exchange)
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

// reconnectConnection attempts to reconnect to the AMQP broker
func (c *MultiTenantConsumer) reconnectConnection(ctx context.Context) error {
	c.logger.Info("Attempting to reconnect to AMQP broker")

	// Set reconnecting flag to prevent concurrent reconnections
	c.reconnecting = true
	defer func() {
		// Will be set to false by caller on success, or here on early return
		if c.reconnecting {
			c.reconnecting = false
		}
	}()

	backoff := 1 * time.Second
	maxBackoff := 30 * time.Second
	maxAttempts := 30

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		// Check circuit breaker
		if c.circuitBreaker.State() == circuitbreaker.StateOpen {
			c.logger.Warn("Circuit breaker is open, waiting",
				zap.Duration("reset_timeout", 30*time.Second))
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(30 * time.Second):
			}
		}

		// Close existing connection if any
		if c.orgEventsConn != nil && !c.orgEventsConn.IsClosed() {
			_ = c.orgEventsConn.Close()
		}

		// Attempt to reconnect
		err := func() error {
			conn, err := amqp.Dial(c.config.BrokerURL)
			if err != nil {
				return err
			}

			ch, err := conn.Channel()
			if err != nil {
				defer func() {
					if err := conn.Close(); err != nil {
						c.logger.Error("Failed to close AMQP connection", zap.Error(err))
					}
				}()
				return err
			}

			c.orgEventsConn = conn
			c.orgEventsChannel = ch

			// Re-register close notifiers
			c.connCloseNotifier = make(chan *amqp.Error, 1)
			c.channelCloseNotifier = make(chan *amqp.Error, 1)
			c.orgEventsConn.NotifyClose(c.connCloseNotifier)
			c.orgEventsChannel.NotifyClose(c.channelCloseNotifier)

			return nil
		}()

		if err == nil {
			c.logger.Info("Successfully reconnected to AMQP broker",
				zap.Int("attempt", attempt))

			// Record success in circuit breaker
			c.circuitBreaker.RecordSuccess()

			// Re-establish all tenant connections
			c.reestablishTenantConnections(ctx)

			return nil
		}

		c.logger.Warn("Reconnection attempt failed",
			zap.Int("attempt", attempt),
			zap.Duration("backoff", backoff),
			zap.Error(err))

		// Record failure in circuit breaker
		c.circuitBreaker.RecordFailure()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}

		// Exponential backoff
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}

	return fmt.Errorf("failed to reconnect after %d attempts", maxAttempts)
}

// reestablishTenantConnections re-establishes connections for all active tenants
func (c *MultiTenantConsumer) reestablishTenantConnections(ctx context.Context) {
	c.tenantMu.RLock()
	tenants := make([]*TenantConsumer, 0, len(c.tenantConsumers))
	for _, consumer := range c.tenantConsumers {
		tenants = append(tenants, consumer)
	}
	c.tenantMu.RUnlock()

	for _, tenant := range tenants {
		// Remove from map so subscribeToOrganization can add it back
		c.tenantMu.Lock()
		delete(c.tenantConsumers, tenant.OrgSlug)
		c.tenantMu.Unlock()

		// Resubscribe
		if err := c.subscribeToOrganization(ctx, tenant.OrgSlug, tenant.Vhost, tenant.QueueName, tenant.Exchange); err != nil {
			c.logger.Error("Failed to resubscribe to tenant",
				zap.String("org", tenant.OrgSlug),
				zap.Error(err))
		}
	}
}

// listenToOrgEvents listens for organization lifecycle events
func (c *MultiTenantConsumer) listenToOrgEvents(ctx context.Context) error {
	var (
		messages <-chan amqp.Delivery
		err      error
		attempt  = 1
	)

	for {
		if err = c.ensureOrgEventsTopology(); err == nil {
			queueName := c.getOrgEventsQueueName()
			consumerTag := fmt.Sprintf("%s-%s", c.orgEventsConfig.ConsumerTag, c.instanceID)

			messages, err = c.orgEventsChannel.Consume(
				queueName,
				consumerTag,
				false, // manual ack for reliability
				false,
				false,
				false,
				nil,
			)
			if err == nil {
				c.logger.Info("Started consuming org events",
					zap.String("queue", queueName),
					zap.String("consumer_tag", consumerTag))
				break
			}
		}

		backoff := time.Duration(attempt)
		if backoff > 10 {
			backoff = 10
		}

		c.logger.Warn("Telemetry org events setup retry",
			zap.Int("attempt", attempt),
			zap.Duration("next_retry", backoff*time.Second),
			zap.Error(err))

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff * time.Second):
		}

		if attempt < 10 {
			attempt++
		}
	}

	// Process org events
	for {
		select {
		case <-ctx.Done():
			return nil
		case msg, ok := <-messages:
			if !ok {
				return nil
			}

			c.logger.Debug("Received org event", zap.String("routing_key", msg.RoutingKey))

			if err := c.handleOrgEvent(ctx, msg); err != nil {
				c.logger.Error("Error handling org event", zap.Error(err))
				_ = msg.Nack(false, true) // Requeue
			} else {
				_ = msg.Ack(false)
			}
		}
	}
}

// handleOrgEvent processes organization lifecycle events
func (c *MultiTenantConsumer) handleOrgEvent(ctx context.Context, msg amqp.Delivery) error {
	c.logger.Info("Processing org event", zap.String("routing_key", msg.RoutingKey))

	var event models.OrgEvent

	if err := json.Unmarshal(msg.Body, &event); err != nil {
		return fmt.Errorf("failed to unmarshal org event: %w", err)
	}

	c.logger.Info("Processing org event details",
		zap.String("event_type", string(event.EventType)),
		zap.String("org", event.Payload.Slug))

	orgSlug := event.Payload.Slug
	vhost := event.Payload.Vhost

	switch event.EventType {
	case models.OrgCreated:
		// New org created - subscribe to its queue
		if vhost == "" {
			c.logger.Warn("Org created event missing vhost", zap.String("org", orgSlug))
			return nil
		}
		// Use empty strings to let subscribeToOrganization create default names
		if err := c.subscribeToOrganization(ctx, orgSlug, vhost, "", ""); err != nil {
			return err
		}

		// Ask processor to ensure any per-organization setup (e.g., DB schema)
		if c.processor != nil {
			if err := c.processor.OnOrgCreated(ctx, orgSlug); err != nil {
				c.logger.Error("Processor failed to handle org creation",
					zap.String("org", orgSlug),
					zap.Error(err))
				return err
			}
		}

		return nil

	case models.OrgDeactivated, models.OrgDeleted:
		// Org deleted/deactivated - unsubscribe
		c.unsubscribeFromOrganization(orgSlug)

		// If this is a deletion event, notify processor to perform cleanup
		if event.EventType == models.OrgDeleted {
			if c.processor != nil {
				if err := c.processor.OnOrgDeleted(ctx, orgSlug); err != nil {
					c.logger.Error("Processor failed to handle org deletion",
						zap.String("org", orgSlug),
						zap.Error(err))
					return err
				}
			}
		}

		return nil

	default:
		c.logger.Warn("Unknown event type", zap.String("event_type", string(event.EventType)))
		return nil
	}
}

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

				// Trigger resubscription for this tenant
				go c.resubscribeTenant(ctx, tenant)
				return
			}

			// Check if the delivery channel is still valid before processing
			// When the AMQP connection closes, the delivery becomes invalid
			if tenant.Channel == nil || tenant.Channel.IsClosed() {
				c.logger.Warn("Tenant channel closed, skipping message processing",
					zap.String("org", tenant.OrgSlug))
				go c.resubscribeTenant(ctx, tenant)
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

// Stop gracefully stops the consumer
func (c *MultiTenantConsumer) Stop() error {
	close(c.done)
	c.stopAllConsumers()

	if c.orgEventsChannel != nil {
		_ = c.orgEventsChannel.Close()
	}

	if c.orgEventsConn != nil {
		_ = c.orgEventsConn.Close()
	}

	return nil
}

// IsHealthy checks if the consumer is healthy
func (c *MultiTenantConsumer) IsHealthy() bool {
	return c.orgEventsConn != nil && !c.orgEventsConn.IsClosed()
}
