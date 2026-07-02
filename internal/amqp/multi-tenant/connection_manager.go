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
	go m.monitorConnection(m.connCloseCh, m.channelCloseCh)
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

func (m *ConnectionManager) monitorConnection(connCloseCh <-chan *amqp.Error, channelCloseCh <-chan *amqp.Error) {
	defer m.monitorWg.Done()
	defer func() {
		if r := recover(); r != nil {
			m.logger.Error("monitorConnection panicked", zap.Any("panic", r))
		}
	}()

	for {
		select {
		case <-m.done:
			return
		case err, ok := <-connCloseCh:
			if !ok {
				return
			}
			m.onConnClosed(err)
		case err, ok := <-channelCloseCh:
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
	defer func() {
		if r := recover(); r != nil {
			m.logger.Error("reconnectLoop panicked", zap.Any("panic", r))
		}
	}()

	m.logger.Info("Reconnect loop started")

	backoff := 1 * time.Second
	const maxBackoff = 30 * time.Second
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-m.done:
			return
		case <-ticker.C:
			if !m.IsConnected() && !m.reconnecting.Load() {
				m.logger.Debug("Ticker detected disconnected state, triggering reconnect")
				select {
				case m.reconnectChan <- struct{}{}:
				default:
				}
			}
		case <-m.reconnectChan:
			if m.reconnecting.Load() || m.IsConnected() {
				m.logger.Debug("Skipping reconnect signal",
					zap.Bool("already_reconnecting", m.reconnecting.Load()),
					zap.Bool("already_connected", m.IsConnected()))
				continue
			}

			m.reconnecting.Store(true)
			m.logger.Warn("Reconnection triggered, attempting to reconnect...")

			go func(b time.Duration) {
				defer func() {
					if r := recover(); r != nil {
						m.logger.Error("reconnect attempt panicked", zap.Any("panic", r))
					}
				}()

				currentBackoff := b

				for {
					if m.circuitBreaker.State() == circuitbreaker.StateOpen {
						timeout := m.circuitBreaker.ResetTimeout()
						m.logger.Warn("Circuit breaker is open, waiting",
							zap.Duration("reset_timeout", timeout))
						select {
						case <-m.done:
							m.reconnecting.Store(false)
							return
						case <-time.After(timeout):
						}
					}

					if err := m.doReconnect(); err != nil {
						m.logger.Warn("Reconnection attempt failed",
							zap.Duration("backoff", currentBackoff),
							zap.Error(err))
						m.circuitBreaker.RecordFailure()

						select {
						case <-m.done:
							m.reconnecting.Store(false)
							return
						case <-time.After(currentBackoff):
						}

						currentBackoff *= 2
						if currentBackoff > maxBackoff {
							currentBackoff = maxBackoff
						}
					} else {
						m.circuitBreaker.Reset()
						m.reconnecting.Store(false)
						m.logger.Info("Successfully reconnected")

						if m.OnReconnected != nil {
							ctx := m.serviceCtx
							if ctx == nil {
								ctx = context.Background()
							}
							m.OnReconnected(ctx)
						}
						return
					}
				}
			}(backoff)

			backoff = 1 * time.Second
		}
	}
}

func (m *ConnectionManager) doReconnect() error {
	m.logger.Info("Attempting to reconnect to AMQP broker")

	m.connMu.RLock()
	oldConn := m.conn
	m.connMu.RUnlock()
	if oldConn != nil && !oldConn.IsClosed() {
		_ = oldConn.Close()
	}

	conn, err := amqp.Dial(m.config.BrokerURL)
	if err != nil {
		return fmt.Errorf("dial failed: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("open channel failed: %w", err)
	}

	m.connMu.Lock()
	m.conn = conn
	m.channel = ch

	m.connCloseCh = make(chan *amqp.Error, 1)
	m.channelCloseCh = make(chan *amqp.Error, 1)
	conn.NotifyClose(m.connCloseCh)
	ch.NotifyClose(m.channelCloseCh)
	m.connMu.Unlock()

	m.monitorWg.Add(1)
	go m.monitorConnection(m.connCloseCh, m.channelCloseCh)

	return nil
}
