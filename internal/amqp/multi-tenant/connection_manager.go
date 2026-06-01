package amqp

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Space-DF/telemetry-service/internal/circuitbreaker"
	"github.com/Space-DF/telemetry-service/internal/config"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

type ConnectionManager struct {
	config         config.AMQP
	connMu         sync.RWMutex
	conn           *amqp.Connection
	channel        *amqp.Channel
	circuitBreaker *circuitbreaker.CircuitBreaker

	done           chan struct{}
	shutdownOnce   sync.Once
	reconnectChan  chan struct{}
	connCloseCh    chan *amqp.Error
	channelCloseCh chan *amqp.Error
	reconnecting   atomic.Bool
	monitorWg      sync.WaitGroup

	logger *zap.Logger

	serviceCtx context.Context

	OnConnectionLost func()
	OnReconnected    func(ctx context.Context)
}

func NewConnectionManager(cfg config.AMQP, logger *zap.Logger) *ConnectionManager {
	cbConfig := circuitbreaker.Config{
		MaxFailures:      5,
		ResetTimeout:     30 * time.Second,
		SuccessThreshold: 2,
	}
	return &ConnectionManager{
		config:         cfg,
		circuitBreaker: circuitbreaker.New(cbConfig),
		done:           make(chan struct{}),
		reconnectChan:  make(chan struct{}, 1),
		logger:         logger,
	}
}

func (m *ConnectionManager) Connect() error {
	return m.circuitBreaker.Execute(func() error {
		var err error
		conn, err := amqp.Dial(m.config.BrokerURL)
		if err != nil {
			return fmt.Errorf("failed to connect to AMQP broker: %w", err)
		}

		ch, err := conn.Channel()
		if err != nil {
			defer func() {
				if closeErr := conn.Close(); closeErr != nil {
					m.logger.Error("Failed to close AMQP connection", zap.Error(closeErr))
				}
			}()
			return fmt.Errorf("failed to open org events channel: %w", err)
		}

		m.connMu.Lock()
		m.conn = conn
		m.channel = ch
		m.connMu.Unlock()

		return nil
	})
}

func (m *ConnectionManager) Channel() *amqp.Channel {
	m.connMu.RLock()
	defer m.connMu.RUnlock()
	return m.channel
}

func (m *ConnectionManager) Conn() *amqp.Connection {
	m.connMu.RLock()
	defer m.connMu.RUnlock()
	return m.conn
}

func (m *ConnectionManager) IsConnected() bool {
	m.connMu.RLock()
	defer m.connMu.RUnlock()
	return m.conn != nil && !m.conn.IsClosed()
}

func (m *ConnectionManager) Close() {
	m.shutdownOnce.Do(func() {
		close(m.done)
		m.connMu.Lock()
		if m.channel != nil {
			_ = m.channel.Close()
		}
		if m.conn != nil {
			_ = m.conn.Close()
		}
		m.connMu.Unlock()
		m.monitorWg.Wait()
	})
}

func (m *ConnectionManager) SetupMonitoring() {
	m.connCloseCh = make(chan *amqp.Error, 1)
	m.channelCloseCh = make(chan *amqp.Error, 1)
	m.conn.NotifyClose(m.connCloseCh)
	m.channel.NotifyClose(m.channelCloseCh)

	m.monitorWg.Add(1)
	go m.monitorConnection()
}

func (m *ConnectionManager) StartReconnectLoop() {
	m.monitorWg.Add(1)
	go m.reconnectLoop()
}

func (m *ConnectionManager) TriggerReconnect() {
	if m.reconnecting.Load() {
		m.logger.Debug("Already reconnecting, skipping duplicate request")
		return
	}
	select {
	case m.reconnectChan <- struct{}{}:
	default:
	}
}

func (m *ConnectionManager) monitorConnection() {
	defer m.monitorWg.Done()

	for {
		select {
		case <-m.done:
			return
		case err, ok := <-m.connCloseCh:
			if !ok {
				return
			}
			m.onConnClosed(err)
		case err, ok := <-m.channelCloseCh:
			if !ok {
				return
			}
			m.onChClosed(err)
		}
	}
}

func (m *ConnectionManager) onConnClosed(err *amqp.Error) {
	if m.reconnecting.Load() {
		m.logger.Debug("Ignoring stale connection close event (already reconnecting)",
			zap.Error(err), zap.Int("code", err.Code))
		return
	}

	m.logger.Error("AMQP connection closed unexpectedly",
		zap.Error(err), zap.Int("code", err.Code))

	if m.OnConnectionLost != nil {
		m.OnConnectionLost()
	}

	select {
	case m.reconnectChan <- struct{}{}:
	default:
	}
}

func (m *ConnectionManager) onChClosed(err *amqp.Error) {
	if m.reconnecting.Load() {
		m.logger.Debug("Ignoring stale channel close event (already reconnecting)",
			zap.Error(err), zap.Int("code", err.Code))
		return
	}

	m.logger.Error("AMQP channel closed unexpectedly",
		zap.Error(err), zap.Int("code", err.Code))

	m.circuitBreaker.RecordFailure()

	select {
	case m.reconnectChan <- struct{}{}:
	default:
	}
}

func (m *ConnectionManager) reconnectLoop() {
	defer m.monitorWg.Done()

	for {
		select {
		case <-m.done:
			return
		case <-m.reconnectChan:
			if m.reconnecting.Load() {
				m.logger.Debug("Already reconnecting, skipping duplicate request")
				continue
			}

			m.reconnecting.Store(true)

			m.logger.Warn("Reconnection triggered, attempting to reconnect...")
			if err := m.doReconnect(); err != nil {
				m.reconnecting.Store(false)
				m.logger.Error("Failed to reconnect", zap.Error(err))
				go func() {
					select {
					case <-time.After(10 * time.Second):
						select {
						case m.reconnectChan <- struct{}{}:
						default:
						}
					case <-m.done:
					}
				}()
			} else {
				m.reconnecting.Store(false)
				for {
					select {
					case _, ok := <-m.reconnectChan:
						if !ok {
							goto drainedDone
						}
					default:
						goto drainedDone
					}
				}
			drainedDone:
				m.logger.Info("Successfully reconnected")

				if m.OnReconnected != nil {
					ctx := m.serviceCtx
					if ctx == nil {
						ctx = context.Background()
					}
					m.OnReconnected(ctx)
				}
			}
		}
	}
}

func (m *ConnectionManager) doReconnect() error {
	m.logger.Info("Attempting to reconnect to AMQP broker")

	backoff := 1 * time.Second
	maxBackoff := 30 * time.Second
	maxAttempts := 30

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if m.circuitBreaker.State() == circuitbreaker.StateOpen {
			cbTimeout := m.circuitBreaker.ResetTimeout()
			m.logger.Warn("Circuit breaker is open, waiting",
				zap.Duration("reset_timeout", cbTimeout))
			select {
			case <-m.done:
				return fmt.Errorf("shutdown during circuit breaker wait")
			case <-time.After(cbTimeout):
			}
		}

		m.connMu.RLock()
		oldConn := m.conn
		m.connMu.RUnlock()
		if oldConn != nil && !oldConn.IsClosed() {
			_ = oldConn.Close()
		}

		err := func() error {
			conn, err := amqp.Dial(m.config.BrokerURL)
			if err != nil {
				return err
			}

			ch, err := conn.Channel()
			if err != nil {
				defer func() {
					if closeErr := conn.Close(); closeErr != nil {
						m.logger.Error("Failed to close AMQP connection", zap.Error(closeErr))
					}
				}()
				return err
			}

			m.connMu.Lock()
			m.conn = conn
			m.channel = ch

			m.connCloseCh = make(chan *amqp.Error, 1)
			m.channelCloseCh = make(chan *amqp.Error, 1)
			m.conn.NotifyClose(m.connCloseCh)
			m.channel.NotifyClose(m.channelCloseCh)
			m.connMu.Unlock()

			return nil
		}()

		if err == nil {
			m.logger.Info("Successfully reconnected to AMQP broker",
				zap.Int("attempt", attempt))

			m.circuitBreaker.Reset()

			m.monitorWg.Add(1)
			go m.monitorConnection()

			return nil
		}

		m.logger.Warn("Reconnection attempt failed",
			zap.Int("attempt", attempt),
			zap.Duration("backoff", backoff),
			zap.Error(err))

		m.circuitBreaker.RecordFailure()

		select {
		case <-m.done:
			return fmt.Errorf("shutdown during reconnection")
		case <-time.After(backoff):
		}

		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}

	return fmt.Errorf("failed to reconnect after %d attempts", maxAttempts)
}
