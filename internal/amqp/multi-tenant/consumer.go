package amqp

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/Space-DF/telemetry-service/internal/config"
	"github.com/Space-DF/telemetry-service/internal/models"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

type MultiTenantConsumer struct {
	connManager *ConnectionManager
	vhostPool   *VhostConnectionPool
	registry    *TenantRegistry
	router      *MessageRouter
	orgListener *OrgEventListener
	logger      *zap.Logger
	instanceID  string
	done        chan bool
	stopOnce    sync.Once
}

func NewMultiTenantConsumer(cfg config.AMQP, orgEventsCfg config.OrgEvents, processor MessageProcessor, schemaInit SchemaInitializer, logger *zap.Logger) *MultiTenantConsumer {
	instanceID := generateInstanceID()

	logger.Info("Creating multi-tenant consumer with unique instance ID",
		zap.String("instance_id", instanceID))

	connManager := NewConnectionManager(cfg, logger)

	vhostPool := NewVhostConnectionPool(cfg.BrokerURL, cfg.AllowedVHosts)

	router := NewMessageRouter(processor, logger)

	registry := NewTenantRegistry(vhostPool, router, instanceID, cfg.PrefetchCount, logger)

	orgListener := NewOrgEventListener(connManager, registry, processor, orgEventsCfg, instanceID, logger)

	// Wire reconnection callbacks
	connManager.OnConnectionLost = func() {
		vhostPool.InvalidateAll()
	}

	connManager.OnReconnected = func(ctx context.Context) {
		registry.ReestablishAll(ctx)
		orgListener.Restart(ctx)
		go func() {
			time.Sleep(2 * time.Second)
			if connManager.IsConnected() {
				orgListener.SendDiscoveryRequest(ctx)
			}
		}()
	}

	return &MultiTenantConsumer{
		connManager: connManager,
		vhostPool:   vhostPool,
		registry:    registry,
		router:      router,
		orgListener: orgListener,
		logger:      logger,
		instanceID:  instanceID,
		done:        make(chan bool, 1),
	}
}

func (c *MultiTenantConsumer) Connect() error {
	if err := c.connManager.Connect(); err != nil {
		return err
	}

	c.connManager.SetupMonitoring()
	c.connManager.StartReconnectLoop()

	c.logger.Info("Successfully connected to AMQP broker")
	return nil
}

func (c *MultiTenantConsumer) Start(ctx context.Context) error {
	c.logger.Info("Starting telemetry service with multi-tenant architecture",
		zap.String("instance_id", c.instanceID))
	c.logger.Info("Waiting for organization events to discover active tenants")

	c.connManager.serviceCtx = ctx

	c.orgListener.Start(ctx)

	go func() {
		time.Sleep(2 * time.Second)
		if c.connManager.IsConnected() {
			c.orgListener.SendDiscoveryRequest(ctx)
		}
	}()

	// Wait for context cancellation or done signal
	select {
	case <-ctx.Done():
		c.logger.Info("Context cancelled, stopping multi-tenant consumer")
	case <-c.done:
		c.logger.Info("Multi-tenant consumer stopped")
	}

	return nil
}

// Stop gracefully stops the consumer
func (c *MultiTenantConsumer) Stop() error {
	c.stopOnce.Do(func() {
		close(c.done)
		c.orgListener.Stop()
		c.registry.StopAll()
		c.connManager.Close()
	})
	return nil
}

// IsHealthy checks if the consumer is healthy
func (c *MultiTenantConsumer) IsHealthy() bool {
	return c.connManager.IsConnected()
}

// PublishEventToDevice publishes an event to the tenant's device queue
func (c *MultiTenantConsumer) PublishEventToDevice(ctx context.Context, event *models.Event, orgSlug string) error {
	tenant, exists := c.registry.Get(orgSlug)
	if !exists {
		return fmt.Errorf("tenant %s not found", orgSlug)
	}

	if tenant.Channel == nil || tenant.Channel.IsClosed() {
		return fmt.Errorf("channel closed for tenant %s", orgSlug)
	}

	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	routingKey := fmt.Sprintf("tenant.%s.broker.space.%s.device.%s.event", orgSlug, event.SpaceSlug, event.DeviceID)

	err = tenant.Channel.PublishWithContext(
		ctx,
		tenant.Exchange,
		routingKey,
		false,
		false,
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Body:         body,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to publish event: %w", err)
	}

	return nil
}
