package amqp

import (
	"context"
	"fmt"
	"time"

	"github.com/Space-DF/telemetry-service/internal/circuitbreaker"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

type pooledConnection struct {
	conn     *amqp.Connection
	refCount int
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
	// Ignore stale close events if we're already reconnecting
	if c.reconnecting {
		c.logger.Debug("Ignoring stale connection close event (already reconnecting)",
			zap.Error(err),
			zap.Int("code", err.Code))
		return
	}

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
	// Ignore stale close events if we're already reconnecting
	if c.reconnecting {
		c.logger.Debug("Ignoring stale channel close event (already reconnecting)",
			zap.Error(err),
			zap.Int("code", err.Code))
		return
	}

	c.logger.Error("AMQP channel closed unexpectedly",
		zap.Error(err),
		zap.Int("code", err.Code))

	// Just trigger full reconnection - it's safer and simpler
	select {
	case c.reconnectChan <- struct{}{}:
	default:
	}
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

			// Set flag before calling reconnectConnection
			c.reconnecting = true

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
				// Successfully reconnected - keep reconnecting flag true briefly
				// to filter out any stale close events from the old connection
				c.logger.Info("Successfully reconnected and re-established tenants")
				// Restart org events listener after successful reconnection
				go func() {
					if err := c.listenToOrgEvents(ctx); err != nil {
						c.logger.Error("Org events listener error after reconnection", zap.Error(err))
					}
				}()
				// Delayed flag reset
				go func() {
					time.Sleep(5 * time.Second)
					c.reconnecting = false
					c.logger.Info("Reconnection window closed, ready for new events")
				}()
			}
		}
	}
}

// reconnectConnection attempts to reconnect to the AMQP broker
func (c *MultiTenantConsumer) reconnectConnection(ctx context.Context) error {
	c.logger.Info("Attempting to reconnect to AMQP broker")

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

			// Reset circuit breaker - connection is working again
			c.circuitBreaker.Reset()

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
