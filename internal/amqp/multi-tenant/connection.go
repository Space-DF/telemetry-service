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

	// Start monitoring goroutine with cancelable context
	monitorCtx, monitorCancel := context.WithCancel(context.Background()) //#nosec G118
	c.monitorCancel = monitorCancel
	c.monitorWg.Add(1)
	go c.monitorConnection(monitorCtx)
}

// monitorConnection monitors the connection and channel for unexpected closures
func (c *MultiTenantConsumer) monitorConnection(ctx context.Context) {
	defer c.monitorWg.Done()

	for {
		select {
		case <-c.done:
			return
		case <-ctx.Done():
			return
		case err, ok := <-c.connCloseNotifier:
			if !ok {
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
	if c.reconnecting.Load() {
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

	// Notify reconnection goroutine (reconnect loop handles failure counting)
	select {
	case c.reconnectChan <- struct{}{}:
	default:
	}
}

// handleChannelClosed handles unexpected channel closure
func (c *MultiTenantConsumer) handleChannelClosed(err *amqp.Error) {
	if c.reconnecting.Load() {
		c.logger.Debug("Ignoring stale channel close event (already reconnecting)",
			zap.Error(err),
			zap.Int("code", err.Code))
		return
	}

	c.logger.Error("AMQP channel closed unexpectedly",
		zap.Error(err),
		zap.Int("code", err.Code))

	// Record failure — channel error means something went wrong
	c.circuitBreaker.RecordFailure()

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
			if c.reconnecting.Load() {
				c.logger.Debug("Already reconnecting, skipping duplicate request")
				continue
			}

			// Set flag before calling reconnectConnection
			c.reconnecting.Store(true)

			c.logger.Warn("Reconnection triggered, attempting to reconnect...")
			if err := c.reconnectConnection(ctx); err != nil {
				c.reconnecting.Store(false)
				c.logger.Error("Failed to reconnect", zap.Error(err))
				// Schedule another reconnection attempt
				go func() {
					select {
					case <-time.After(10 * time.Second):
						select {
						case c.reconnectChan <- struct{}{}:
						default:
						}
					case <-ctx.Done():
					}
				}()
			} else {
				// Successfully reconnected — clear flag immediately so org events
				// are not blocked. reestablishTenantConnections already resubscribed.
				c.reconnecting.Store(false)
				// Drain stale reconnection requests
				for {
					select {
					case _, ok := <-c.reconnectChan:
						if !ok {
							goto done
						}
					default:
						goto done
					}
				}
			done:
				c.logger.Info("Successfully reconnected and re-established tenants")

				// Cancel old org events listener (may be retrying on dead channel)
				if c.orgEventsCancel != nil {
					c.orgEventsCancel()
				}

				// Restart org events listener after successful reconnection
				orgEventsCtx, orgEventsCancel := context.WithCancel(ctx) //#nosec G118
				c.orgEventsCancel = orgEventsCancel
				go func() {
					if err := c.listenToOrgEvents(orgEventsCtx); err != nil {
						c.logger.Error("Org events listener error after reconnection", zap.Error(err))
					}
				}()

				// Re-send discovery request on a separate channel to avoid racing
				// with listenToOrgEvents on orgEventsChannel
				go func() {
					time.Sleep(2 * time.Second)
					conn := c.orgEventsConn
					if conn == nil || conn.IsClosed() {
						return
					}
					if err := c.sendDiscoveryRequestOnConn(ctx, conn); err != nil {
						c.logger.Error("Failed to send discovery request after reconnection", zap.Error(err))
					}
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
			cbTimeout := c.circuitBreaker.ResetTimeout()
			c.logger.Warn("Circuit breaker is open, waiting",
				zap.Duration("reset_timeout", cbTimeout))
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(cbTimeout):
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

			// Cancel and wait for old monitor goroutine to exit
			if c.monitorCancel != nil {
				c.monitorCancel()
				c.monitorWg.Wait()
			}

			// Start new monitor goroutine to watch the new notifier channels
			monitorCtx, monitorCancel := context.WithCancel(ctx) //#nosec G118
			c.monitorCancel = monitorCancel
			c.monitorWg.Add(1)
			go c.monitorConnection(monitorCtx)

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

	successCount := 0
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
		} else {
			successCount++
		}
	}

	c.logger.Info("Re-established tenant connections",
		zap.Int("success", successCount),
		zap.Int("total", len(tenants)))
}
