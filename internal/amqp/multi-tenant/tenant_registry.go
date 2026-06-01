package amqp

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Space-DF/telemetry-service/internal/timescaledb"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

type TenantRegistry struct {
	mu            sync.RWMutex
	tenants       map[string]*TenantConsumer
	vhostPool     *VhostConnectionPool
	router        *MessageRouter
	instanceID    string
	prefetchCount int
	logger        *zap.Logger
}

func NewTenantRegistry(vhostPool *VhostConnectionPool, router *MessageRouter, instanceID string, prefetchCount int, logger *zap.Logger) *TenantRegistry {
	return &TenantRegistry{
		tenants:       make(map[string]*TenantConsumer),
		vhostPool:     vhostPool,
		router:        router,
		instanceID:    instanceID,
		prefetchCount: prefetchCount,
		logger:        logger,
	}
}

func (r *TenantRegistry) Exists(orgSlug string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, exists := r.tenants[orgSlug]
	return exists
}

func (r *TenantRegistry) Get(orgSlug string) (*TenantConsumer, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	tc, exists := r.tenants[orgSlug]
	return tc, exists
}

func (r *TenantRegistry) List() []*TenantConsumer {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]*TenantConsumer, 0, len(r.tenants))
	for _, tc := range r.tenants {
		result = append(result, tc)
	}
	return result
}

func (r *TenantRegistry) Subscribe(ctx context.Context, orgSlug, vhost, queueName, exchange string) error {
	if !r.vhostPool.ShouldHandle(vhost) {
		r.logger.Info("Skipping subscription; vhost not assigned",
			zap.String("org", orgSlug), zap.String("vhost", vhost))
		return nil
	}

	if queueName == "" {
		queueName = fmt.Sprintf("%s.telemetry.queue", orgSlug)
	}
	if exchange == "" {
		exchange = fmt.Sprintf("%s.exchange", orgSlug)
	}

	if r.Exists(orgSlug) {
		r.logger.Info("Subscription already active",
			zap.String("org", orgSlug), zap.String("vhost", vhost))
		return nil
	}

	conn, err := r.vhostPool.Get(vhost)
	if err != nil {
		return err
	}

	channel, err := conn.Channel()
	if err != nil {
		r.vhostPool.Release(vhost)
		return fmt.Errorf("failed to open channel for vhost %s: %w", vhost, err)
	}

	if err := channel.Qos(r.prefetchCount, 0, false); err != nil {
		_ = channel.Close()
		r.vhostPool.Release(vhost)
		return fmt.Errorf("failed to set QoS for vhost %s: %w", vhost, err)
	}

	if err := channel.QueueBind(
		queueName,
		fmt.Sprintf("tenant.%s.telemetry.#", orgSlug),
		exchange,
		false,
		nil,
	); err != nil {
		_ = channel.Close()
		r.vhostPool.Release(vhost)
		return fmt.Errorf("failed to bind queue '%s' to exchange '%s': %w", queueName, exchange, err)
	}

	consumerTag := r.makeConsumerTag(orgSlug, vhost)
	messages, err := channel.Consume(
		queueName,
		consumerTag,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		_ = channel.Close()
		r.vhostPool.Release(vhost)
		return fmt.Errorf("failed to start consuming from queue '%s' in vhost '%s': %w", queueName, vhost, err)
	}

	tenantCtx, cancel := context.WithCancel(ctx) //#nosec G118
	consumer := &TenantConsumer{
		OrgSlug:     orgSlug,
		Vhost:       vhost,
		QueueName:   queueName,
		Exchange:    exchange,
		ConsumerTag: consumerTag,
		Channel:     channel,
		Cancel:      cancel,
		ParentCtx:   ctx,
	}

	r.mu.Lock()
	r.tenants[orgSlug] = consumer
	r.mu.Unlock()

	go r.processTenantMessages(tenantCtx, consumer, messages)

	r.logger.Info("Started consuming from organization",
		zap.String("org", orgSlug),
		zap.String("vhost", vhost),
		zap.String("queue", queueName))
	return nil
}

func (r *TenantRegistry) Unsubscribe(orgSlug string) {
	r.mu.Lock()
	consumer, exists := r.tenants[orgSlug]
	if exists {
		delete(r.tenants, orgSlug)
	}
	r.mu.Unlock()

	if !exists || consumer == nil {
		return
	}

	consumer.Cancel()

	if consumer.Channel != nil {
		_ = consumer.Channel.Cancel(consumer.ConsumerTag, false)
		_ = consumer.Channel.Close()
	}

	r.vhostPool.Release(consumer.Vhost)

	r.logger.Info("Stopped consuming from organization",
		zap.String("org", consumer.OrgSlug),
		zap.String("vhost", consumer.Vhost),
		zap.String("queue", consumer.QueueName))
}

func (r *TenantRegistry) StopAll() {
	r.logger.Info("Stopping all tenant consumers")
	r.mu.Lock()
	for slug, consumer := range r.tenants {
		if consumer != nil {
			consumer.Cancel()
			if consumer.Channel != nil {
				_ = consumer.Channel.Cancel(consumer.ConsumerTag, false)
				_ = consumer.Channel.Close()
			}
			r.vhostPool.Release(consumer.Vhost)
			r.logger.Info("Stopped consuming from organization",
				zap.String("org", consumer.OrgSlug),
				zap.String("vhost", consumer.Vhost))
		}
		delete(r.tenants, slug)
	}
	r.mu.Unlock()

	r.vhostPool.InvalidateAll()
	r.logger.Info("All tenant consumers stopped")
}

func (r *TenantRegistry) ReestablishAll(ctx context.Context) {
	r.mu.RLock()
	tenants := make([]*TenantConsumer, 0, len(r.tenants))
	for _, tc := range r.tenants {
		tenants = append(tenants, tc)
	}
	r.mu.RUnlock()

	successCount := 0
	for _, tenant := range tenants {
		r.mu.Lock()
		delete(r.tenants, tenant.OrgSlug)
		r.mu.Unlock()

		if err := r.Subscribe(ctx, tenant.OrgSlug, tenant.Vhost, tenant.QueueName, tenant.Exchange); err != nil {
			r.logger.Error("Failed to resubscribe to tenant",
				zap.String("org", tenant.OrgSlug),
				zap.Error(err))
		} else {
			successCount++
		}
	}

	r.logger.Info("Re-established tenant connections",
		zap.Int("success", successCount),
		zap.Int("total", len(tenants)))
}

func (r *TenantRegistry) Resubscribe(parentCtx context.Context, oldTenant *TenantConsumer) {
	oldTenant.Cancel()
	if oldTenant.Channel != nil {
		_ = oldTenant.Channel.Cancel(oldTenant.ConsumerTag, false)
		_ = oldTenant.Channel.Close()
	}
	r.vhostPool.Release(oldTenant.Vhost)

	backoff := 1 * time.Second
	maxBackoff := 30 * time.Second
	maxAttempts := 5

	for attempt := 0; attempt < maxAttempts; attempt++ {
		select {
		case <-parentCtx.Done():
			return
		case <-time.After(backoff):
		}

		r.mu.Lock()
		current, exists := r.tenants[oldTenant.OrgSlug]
		if exists && current != oldTenant {
			r.mu.Unlock()
			return
		}

		delete(r.tenants, oldTenant.OrgSlug)
		r.mu.Unlock()

		err := r.Subscribe(parentCtx, oldTenant.OrgSlug, oldTenant.Vhost, oldTenant.QueueName, oldTenant.Exchange)
		if err == nil {
			r.logger.Info("Successfully resubscribed tenant",
				zap.String("org", oldTenant.OrgSlug),
				zap.Int("attempt", attempt+1))
			return
		}

		r.logger.Warn("Failed to resubscribe to tenant",
			zap.String("org", oldTenant.OrgSlug),
			zap.Int("attempt", attempt+1),
			zap.Error(err))

		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}

	r.mu.Lock()
	delete(r.tenants, oldTenant.OrgSlug)
	r.mu.Unlock()

	r.logger.Warn("Failed to resubscribe tenant after all attempts, removed from map for global reconnection",
		zap.String("org", oldTenant.OrgSlug))
}

func (r *TenantRegistry) makeConsumerTag(orgSlug, vhost string) string {
	safeVhost := strings.NewReplacer("/", "_", ".", "_", ":", "_").Replace(vhost)
	return fmt.Sprintf("telemetry-%s-%s-%s", orgSlug, safeVhost, r.instanceID)
}

func (r *TenantRegistry) processTenantMessages(ctx context.Context, tenant *TenantConsumer, messages <-chan amqp.Delivery) {
	r.logger.Info("Processing messages for organization", zap.String("org", tenant.OrgSlug))

	for {
		select {
		case <-ctx.Done():
			r.logger.Info("Stopping message processing for organization",
				zap.String("org", tenant.OrgSlug))
			return

		case msg, ok := <-messages:
			if !ok {
				if ctx.Err() != nil {
					r.logger.Info("Context cancelled, stopping message processing",
						zap.String("org", tenant.OrgSlug))
					return
				}
				r.logger.Warn("Message channel closed for organization, triggering resubscription",
					zap.String("org", tenant.OrgSlug))

				go r.Resubscribe(tenant.ParentCtx, tenant)
				return
			}

			if tenant.Channel == nil || tenant.Channel.IsClosed() {
				r.logger.Warn("Tenant channel closed, skipping message processing",
					zap.String("org", tenant.OrgSlug))
				go r.Resubscribe(tenant.ParentCtx, tenant)
				return
			}

			orgSlug := extractOrgFromRoutingKey(msg.RoutingKey)
			if orgSlug == "" {
				orgSlug = tenant.OrgSlug
			}
			orgCtx := timescaledb.ContextWithOrg(ctx, orgSlug)

			r.router.RouteMessage(orgCtx, orgSlug, tenant, msg)
		}
	}
}
