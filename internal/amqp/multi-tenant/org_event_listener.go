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

type OrgEventListener struct {
	connManager *ConnectionManager
	registry    *TenantRegistry
	processor   MessageProcessor
	config      config.OrgEvents
	instanceID  string
	cancelMu    sync.Mutex
	cancel      context.CancelFunc
	logger      *zap.Logger
}

func NewOrgEventListener(connManager *ConnectionManager, registry *TenantRegistry, processor MessageProcessor, cfg config.OrgEvents, instanceID string, logger *zap.Logger) *OrgEventListener {
	return &OrgEventListener{
		connManager: connManager,
		registry:    registry,
		processor:   processor,
		config:      cfg,
		instanceID:  instanceID,
		logger:      logger,
	}
}

func (l *OrgEventListener) queueName() string {
	return fmt.Sprintf("%s.%s", l.config.Queue, l.instanceID)
}

func (l *OrgEventListener) Start(ctx context.Context) {
	innerCtx, cancel := context.WithCancel(ctx)
	l.cancelMu.Lock()
	l.cancel = cancel
	l.cancelMu.Unlock()
	go l.listen(ctx, innerCtx)
}

func (l *OrgEventListener) Restart(parentCtx context.Context) {
	l.cancelMu.Lock()
	if l.cancel != nil {
		l.cancel()
	}
	ctx, cancel := context.WithCancel(parentCtx)
	l.cancel = cancel
	l.cancelMu.Unlock()
	go l.listen(parentCtx, ctx)
}

func (l *OrgEventListener) Stop() {
	l.cancelMu.Lock()
	if l.cancel != nil {
		l.cancel()
	}
	l.cancelMu.Unlock()
}

func (l *OrgEventListener) SendDiscoveryRequest(ctx context.Context) {
	if err := l.sendDiscoveryRequestOnConn(ctx, l.connManager.Conn()); err != nil {
		l.logger.Error("Failed to send discovery request", zap.Error(err))
	}
}

func (l *OrgEventListener) listen(parentCtx context.Context, myCtx context.Context) {
	var (
		messages <-chan amqp.Delivery
		err      error
		attempt  = 1
	)

	for {
		if err = l.ensureTopology(); err == nil {
			queueName := l.queueName()
			consumerTag := fmt.Sprintf("%s-%s", l.config.ConsumerTag, l.instanceID)

			messages, err = l.connManager.Channel().Consume(
				queueName,
				consumerTag,
				false,
				false,
				false,
				false,
				nil,
			)
			if err == nil {
				l.logger.Info("Started consuming org events",
					zap.String("queue", queueName),
					zap.String("consumer_tag", consumerTag))
				break
			}
		}

		backoff := time.Duration(attempt)
		if backoff > 10 {
			backoff = 10
		}

		l.logger.Warn("Telemetry org events setup retry",
			zap.Int("attempt", attempt),
			zap.Duration("next_retry", backoff*time.Second),
			zap.Error(err))

		select {
		case <-myCtx.Done():
			return
		case <-time.After(backoff * time.Second):
		}

		if attempt < 10 {
			attempt++
		}
	}

	for {
		select {
		case <-myCtx.Done():
			return
		case msg, ok := <-messages:
			if !ok {
				return
			}

			l.logger.Debug("Received org event", zap.String("routing_key", msg.RoutingKey))

			if err := l.handleOrgEvent(parentCtx, msg); err != nil {
				l.logger.Error("Error handling org event", zap.Error(err))
				_ = msg.Nack(false, true)
			} else {
				_ = msg.Ack(false)
			}
		}
	}
}

func (l *OrgEventListener) ensureTopology() error {
	channel := l.connManager.Channel()
	if channel == nil || channel.IsClosed() {
		return fmt.Errorf("org events channel is nil or closed")
	}

	if err := channel.ExchangeDeclare(
		l.config.Exchange,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("exchange declare failed: %w", err)
	}

	queueName := l.queueName()

	if _, err := channel.QueueDeclare(
		queueName,
		false,
		true,
		true,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("queue declare failed for %s: %w", queueName, err)
	}

	if err := channel.QueueBind(
		queueName,
		l.config.RoutingKey,
		l.config.Exchange,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("queue bind failed for %s: %w", queueName, err)
	}

	l.logger.Info("Org events topology configured",
		zap.String("queue", queueName),
		zap.String("exchange", l.config.Exchange),
		zap.String("routing_key", l.config.RoutingKey))

	return nil
}

func (l *OrgEventListener) handleOrgEvent(ctx context.Context, msg amqp.Delivery) error {
	l.logger.Info("Processing org event", zap.String("routing_key", msg.RoutingKey))

	var event models.OrgEvent
	if err := json.Unmarshal(msg.Body, &event); err != nil {
		return fmt.Errorf("failed to unmarshal org event: %w", err)
	}

	l.logger.Info("Processing org event details",
		zap.String("event_type", string(event.EventType)),
		zap.String("org", event.Payload.Slug))

	orgSlug := event.Payload.Slug
	vhost := event.Payload.Vhost

	switch event.EventType {
	case models.OrgCreated:
		if vhost == "" {
			l.logger.Warn("Org created event missing vhost", zap.String("org", orgSlug))
			return nil
		}
		if l.registry.Exists(orgSlug) {
			l.logger.Debug("Skipping org.created: already subscribed",
				zap.String("org", orgSlug))
			return nil
		}
		queueName := event.Payload.TelemetryQueue
		if queueName == "" && orgSlug != "" {
			queueName = fmt.Sprintf("%s.telemetry.queue", orgSlug)
		}
		if err := l.registry.Subscribe(ctx, orgSlug, vhost, queueName, ""); err != nil {
			return err
		}
		if l.processor != nil {
			if err := l.processor.OnOrgCreated(ctx, orgSlug); err != nil {
				l.logger.Error("Processor failed to handle org creation",
					zap.String("org", orgSlug), zap.Error(err))
				return err
			}
		}
		return nil

	case models.OrgDeactivated, models.OrgDeleted:
		l.registry.Unsubscribe(orgSlug)
		if event.EventType == models.OrgDeleted {
			if l.processor != nil {
				if err := l.processor.OnOrgDeleted(ctx, orgSlug); err != nil {
					l.logger.Error("Processor failed to handle org deletion",
						zap.String("org", orgSlug), zap.Error(err))
					return err
				}
			}
		}
		return nil

	default:
		l.logger.Warn("Unknown event type", zap.String("event_type", string(event.EventType)))
		return nil
	}
}

func (l *OrgEventListener) sendDiscoveryRequestOnConn(ctx context.Context, conn *amqp.Connection) error {
	l.logger.Info("Sending discovery request to console service for existing organizations",
		zap.String("reply_to", l.queueName()))

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open channel for discovery request: %w", err)
	}
	defer func() {
		if closeErr := ch.Close(); closeErr != nil {
			l.logger.Error("Failed to close channel after sending discovery request", zap.Error(closeErr))
		}
	}()

	request := models.OrgDiscoveryRequest{
		EventType:   models.OrgDiscoveryReq,
		EventID:     fmt.Sprintf("discovery-%s-%d", l.instanceID, time.Now().Unix()),
		Timestamp:   time.Now(),
		ServiceName: "telemetry-service",
		ReplyTo:     l.queueName(),
	}

	body, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("failed to marshal discovery request: %w", err)
	}

	err = ch.PublishWithContext(
		ctx,
		l.config.Exchange,
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

	l.logger.Info("Discovery request sent successfully")
	return nil
}
