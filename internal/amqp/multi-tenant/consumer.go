package amqp

import (
	"context"
	"time"

	"go.uber.org/zap"
)

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

// PublishEventToDevice publishes an event to the tenant's device queue
func (c *MultiTenantConsumer) PublishEventToDevice(ctx context.Context, event *models.Event, orgSlug string) error {
	c.tenantMu.RLock()
	consumer, exists := c.tenantConsumers[orgSlug]
	c.tenantMu.RUnlock()

	if !exists {
		return fmt.Errorf("tenant %s not found", orgSlug)
	}

	if consumer.Channel == nil || consumer.Channel.IsClosed() {
		return fmt.Errorf("channel closed for tenant %s", orgSlug)
	}

	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	routingKey := fmt.Sprintf("tenant.%s.space.%s.device.%s.events", orgSlug, event.SpaceSlug, event.DeviceID)

	err = consumer.Channel.PublishWithContext(
		ctx,
		consumer.Exchange,
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
