package amqp

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Space-DF/telemetry-service/internal/models"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

func (c *MultiTenantConsumer) getOrgEventsQueueName() string {
	return fmt.Sprintf("%s.%s", c.orgEventsConfig.Queue, c.instanceID)
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

// sendDiscoveryRequest sends a request to console service to get all active orgs
func (c *MultiTenantConsumer) sendDiscoveryRequest(ctx context.Context) error {
	return c.sendDiscoveryRequestOnConn(ctx, c.orgEventsConn)
}

func (c *MultiTenantConsumer) sendDiscoveryRequestOnConn(ctx context.Context, conn *amqp.Connection) error {
	c.logger.Info("Sending discovery request to console service for existing organizations",
		zap.String("reply_to", c.getOrgEventsQueueName()))

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open channel for discovery request: %w", err)
	}
	defer func() {
		if err := ch.Close(); err != nil {
			c.logger.Error("Failed to close channel after sending discovery request", zap.Error(err))
		}
	}()

	request := models.OrgDiscoveryRequest{
		EventType:   models.OrgDiscoveryReq,
		EventID:     fmt.Sprintf("discovery-%s-%d", c.instanceID, time.Now().Unix()),
		Timestamp:   time.Now(),
		ServiceName: "telemetry-service",
		ReplyTo:     c.getOrgEventsQueueName(),
	}

	body, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("failed to marshal discovery request: %w", err)
	}

	err = ch.PublishWithContext(
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
		if vhost == "" {
			c.logger.Warn("Org created event missing vhost", zap.String("org", orgSlug))
			return nil
		}
		c.tenantMu.RLock()
		_, already := c.tenantConsumers[orgSlug]
		c.tenantMu.RUnlock()
		if already {
			c.logger.Debug("Skipping org.created: already subscribed",
				zap.String("org", orgSlug))
			return nil
		}
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
