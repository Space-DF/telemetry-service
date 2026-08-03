package celery

import (
	"fmt"

	"github.com/Space-DF/telemetry-service/internal/celery/topology"
	amqp "github.com/rabbitmq/amqp091-go"
)

// declareTopology declares the exchange, queue and binding for a single spec.
// On failure it tears down the channel/connection so a reconnect can rebuild
// from scratch. Exchange declaration is skipped when exchange == "" (queue
// bound to the default exchange).
func (c *TaskConsumer) declareTopology(s topology.Spec) error {
	if s.Exchange != "" {
		if err := c.channel.ExchangeDeclare(
			s.Exchange,
			s.ExchangeType,
			true,  // durable
			false, // auto-deleted
			false, // internal
			false, // noWait
			nil,
		); err != nil {
			c.tearDown()
			return fmt.Errorf("failed to declare %s exchange: %w", s.Exchange, err)
		}
	}

	queueArgs := amqp.Table{"x-single-active-consumer": true}
	if _, err := c.channel.QueueDeclare(
		s.Queue,
		true,  // durable
		false, // auto-deleted
		false, // exclusive
		false, // noWait
		queueArgs,
	); err != nil {
		c.tearDown()
		return fmt.Errorf("failed to declare queue %s: %w", s.Queue, err)
	}

	if err := c.channel.QueueBind(
		s.Queue,
		s.RoutingKey,
		s.Exchange,
		false, // noWait
		nil,
	); err != nil {
		c.tearDown()
		return fmt.Errorf("failed to bind queue %s: %w", s.Queue, err)
	}
	return nil
}

// tearDown closes the channel and connection after a topology error so the
// next Connect() starts clean. Fields are nilled so IsHealthy reflects the
// closed state during the reconnect window rather than reporting a stale
// closed-but-non-nil connection as healthy.
func (c *TaskConsumer) tearDown() {
	if c.channel != nil {
		_ = c.channel.Close()
		c.channel = nil
	}
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}
}
