package celery

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/Space-DF/telemetry-service/internal/models"
	"github.com/Space-DF/telemetry-service/internal/timescaledb"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

const (
	// Exchange names as defined in common/celery/routing.py
	UpdateSpaceExchange  = "update_space"
	DeleteSpaceExchange  = "delete_space"
	DeleteDeviceExchange = "delete_device"

	// Task names for message identification
	UpdateSpaceTaskName  = "spacedf.tasks.update_space"
	DeleteSpaceTaskName  = "spacedf.tasks.delete_space"
	DeleteDeviceTaskName = "spacedf.tasks.delete_device"
)

// TaskConsumer consumes Celery tasks from RabbitMQ
type TaskConsumer struct {
	amqpURL  string
	dbClient *timescaledb.Client
	logger   *zap.Logger
	conn     *amqp.Connection
	channel  *amqp.Channel
	done     chan bool
	wg       sync.WaitGroup
	stopOnce sync.Once

	updateQueueName string
	deleteQueueName string
	deviceQueueName string
}

// SchemaInitializer handles database schema initialization
type SchemaInitializer interface {
	CreateSchemaAndTables(ctx context.Context, orgSlug string) error
}

// NewTaskConsumer creates a new Celery task consumer
func NewTaskConsumer(amqpURL string, dbClient *timescaledb.Client, logger *zap.Logger) *TaskConsumer {
	return &TaskConsumer{
		amqpURL:         amqpURL,
		dbClient:        dbClient,
		logger:          logger,
		done:            make(chan bool, 1),
		updateQueueName: "telemetry_update_space",
		deleteQueueName: "telemetry_delete_space",
		deviceQueueName: "telemetry_delete_device",
	}
}

// Connect establishes connection to RabbitMQ for Celery tasks
func (c *TaskConsumer) Connect() error {
	var err error

	c.conn, err = amqp.Dial(c.amqpURL)
	if err != nil {
		return fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	c.channel, err = c.conn.Channel()
	if err != nil {
		defer func() {
			_ = c.conn.Close()
		}()
		return fmt.Errorf("failed to open channel: %w", err)
	}

	// Set QoS
	if err := c.channel.Qos(10, 0, false); err != nil {
		defer func() {
			_ = c.channel.Close()
			_ = c.conn.Close()
		}()
		return fmt.Errorf("failed to set QoS: %w", err)
	}

	// Declare exchanges (following common/celery/routing.py pattern)
	// Exchange: update_space (fanout type)
	err = c.channel.ExchangeDeclare(
		UpdateSpaceExchange,
		"fanout", // fanout type for broadcasting to all services
		true,     // durable
		false,
		false,
		false, // wait
		nil,
	)
	if err != nil {
		defer func() {
			_ = c.channel.Close()
			_ = c.conn.Close()
		}()
		return fmt.Errorf("failed to declare update_space exchange: %w", err)
	}

	// Exchange: delete_space (fanout type)
	err = c.channel.ExchangeDeclare(
		DeleteSpaceExchange,
		"fanout",
		true,
		false,
		false,
		true, // wait
		nil,
	)
	if err != nil {
		defer func() {
			_ = c.channel.Close()
			_ = c.conn.Close()
		}()
		return fmt.Errorf("failed to declare delete_space exchange: %w", err)
	}

	// Exchange: delete_device (fanout type)
	err = c.channel.ExchangeDeclare(
		DeleteDeviceExchange,
		"fanout",
		true,
		false,
		false,
		true,
		nil,
	)
	if err != nil {
		defer func() {
			_ = c.channel.Close()
			_ = c.conn.Close()
		}()
		return fmt.Errorf("failed to declare delete_device exchange: %w", err)
	}

	// Declare update queue
	_, err = c.channel.QueueDeclare(
		c.updateQueueName,
		true,  // durable
		false, // auto-delete when unused
		false, // non-exclusive
		false, // no-wait
		amqp.Table{
			"x-single-active-consumer": true,
		},
	)
	if err != nil {
		defer func() {
			_ = c.channel.Close()
			_ = c.conn.Close()
		}()
		return fmt.Errorf("failed to declare update queue: %w", err)
	}

	// Declare delete queue
	_, err = c.channel.QueueDeclare(
		c.deleteQueueName,
		true,  // durable
		false, // auto-delete when unused
		false, // non-exclusive
		false, // no-wait
		amqp.Table{
			"x-single-active-consumer": true,
		},
	)
	if err != nil {
		defer func() {
			_ = c.channel.Close()
			_ = c.conn.Close()
		}()
		return fmt.Errorf("failed to declare delete queue: %w", err)
	}

	// Declare device queue
	_, err = c.channel.QueueDeclare(
		c.deviceQueueName,
		true,
		false,
		false,
		false,
		amqp.Table{
			"x-single-active-consumer": true,
		},
	)
	if err != nil {
		defer func() {
			_ = c.channel.Close()
			_ = c.conn.Close()
		}()
		return fmt.Errorf("failed to declare device queue: %w", err)
	}

	// Bind update queue to update_space exchange
	if err := c.channel.QueueBind(
		c.updateQueueName,
		UpdateSpaceExchange,
		UpdateSpaceExchange,
		false,
		nil,
	); err != nil {
		_ = c.channel.Close()
		_ = c.conn.Close()
		return fmt.Errorf("failed to bind update queue: %w", err)
	}

	// Bind delete queue to delete_space exchange
	if err := c.channel.QueueBind(
		c.deleteQueueName,
		DeleteSpaceExchange,
		DeleteSpaceExchange,
		false,
		nil,
	); err != nil {
		_ = c.channel.Close()
		_ = c.conn.Close()
		return fmt.Errorf("failed to bind delete queue: %w", err)
	}

	// Bind device queue to delete_device exchange
	if err := c.channel.QueueBind(
		c.deviceQueueName,
		DeleteDeviceExchange,
		DeleteDeviceExchange,
		false,
		nil,
	); err != nil {
		_ = c.channel.Close()
		_ = c.conn.Close()
		return fmt.Errorf("failed to bind device queue: %w", err)
	}

	c.logger.Info("Celery task consumer connected",
		zap.String("update_queue", c.updateQueueName),
		zap.String("delete_queue", c.deleteQueueName),
		zap.String("device_queue", c.deviceQueueName))

	return nil
}

// Start begins consuming Celery tasks and handles reconnection
func (c *TaskConsumer) Start(ctx context.Context) error {
	for {
		if err := c.connectAndConsume(ctx); err != nil {
			c.logger.Error("Celery consumer error, will reconnect", zap.Error(err))
		}

		// Wait for context cancellation or reconnect
		select {
		case <-ctx.Done():
			c.logger.Info("Celery task consumer context cancelled")
			return nil
		case <-c.done:
			c.logger.Info("Celery task consumer stopped")
			return nil
		default:
		}

		// Reconnect with backoff
		backoff := 1 * time.Second
		maxBackoff := 30 * time.Second
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-c.done:
				return nil
			case <-time.After(backoff):
			}

			c.logger.Info("Celery consumer attempting reconnection", zap.Duration("backoff", backoff))

			if err := c.reconnect(); err != nil {
				c.logger.Error("Celery consumer reconnection failed", zap.Error(err))
				backoff *= 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
				continue
			}

			break
		}
	}
}

// connectAndConsume sets up consumption on the current connection
func (c *TaskConsumer) connectAndConsume(ctx context.Context) error {
	// Consume from update queue
	updateMessages, err := c.channel.Consume(
		c.updateQueueName,
		"telemetry_update_consumer",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to start consuming update queue: %w", err)
	}

	// Consume from delete queue
	deleteMessages, err := c.channel.Consume(
		c.deleteQueueName,
		"telemetry_delete_consumer",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to start consuming delete queue: %w", err)
	}

	// Consume from device queue
	deviceMessages, err := c.channel.Consume(
		c.deviceQueueName,
		"telemetry_device_consumer",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to start consuming device queue: %w", err)
	}

	c.logger.Info("Celery task consumer started",
		zap.String("update_queue", c.updateQueueName),
		zap.String("delete_queue", c.deleteQueueName),
		zap.String("device_queue", c.deviceQueueName))

	// Start goroutines for each queue
	c.wg.Add(3)
	go func() {
		defer c.wg.Done()
		c.processMessages(ctx, updateMessages, UpdateSpaceTaskName)
	}()
	go func() {
		defer c.wg.Done()
		c.processMessages(ctx, deleteMessages, DeleteSpaceTaskName)
	}()
	go func() {
		defer c.wg.Done()
		c.processMessages(ctx, deviceMessages, DeleteDeviceTaskName)
	}()

	// Wait for all goroutines to finish (they exit when channel closes)
	c.wg.Wait()

	return nil
}

// reconnect closes the existing connection and establishes a new one
func (c *TaskConsumer) reconnect() error {
	if c.channel != nil {
		if err := c.channel.Close(); err != nil {
			c.logger.Error("Failed to close channel", zap.Error(err))
		}
	}
	if c.conn != nil {
		if err := c.conn.Close(); err != nil {
			c.logger.Error("Failed to close connection", zap.Error(err))
		}
	}
	return c.Connect()
}

// processMessages processes incoming Celery task messages
func (c *TaskConsumer) processMessages(ctx context.Context, messages <-chan amqp.Delivery, expectedTaskName string) {
	for {
		select {
		case <-ctx.Done():
			c.logger.Info("Celery task consumer context cancelled")
			return

		case <-c.done:
			c.logger.Info("Celery task consumer stopped")
			return

		case msg, ok := <-messages:
			if !ok {
				c.logger.Info("Celery task message channel closed")
				return
			}

			c.logger.Debug("Received Celery task",
				zap.String("expected_task", expectedTaskName),
				zap.String("exchange", msg.Exchange))

			if err := c.handleTask(ctx, expectedTaskName, msg.Body); err != nil {
				c.logger.Error("Failed to handle Celery task",
					zap.String("task", expectedTaskName),
					zap.Error(err))
				// Negative ack - requeue so the message is retried
				_ = msg.Nack(false, true)
			} else {
				_ = msg.Ack(false)
			}
		}
	}
}

// handleTask processes a single Celery task
func (c *TaskConsumer) handleTask(ctx context.Context, taskName string, body []byte) error {
	switch taskName {
	case UpdateSpaceTaskName, "update_space":
		return c.handleUpdateSpace(ctx, body)

	case DeleteSpaceTaskName, "delete_space":
		return c.handleDeleteSpace(ctx, body)

	case DeleteDeviceTaskName, "delete_device":
		return c.handleDeleteDevice(ctx, body)

	default:
		c.logger.Debug("Unknown task name, ignoring", zap.String("task", taskName))
		return nil
	}
}

// handleUpdateSpace handles the update_space Celery task
func (c *TaskConsumer) handleUpdateSpace(ctx context.Context, body []byte) error {
	// Format that sent from the django celery is different
	// Parse Celery message format: [args, kwargs, metadata]
	var celeryMsg models.CeleryMessage
	if err := json.Unmarshal(body, &celeryMsg); err != nil {
		return fmt.Errorf("failed to unmarshal celery message: %w", err)
	}

	c.logger.Info("Celery kwargs", zap.String("kwargs", string(celeryMsg.Kwargs)))

	var task models.UpdateSpaceTask
	if err := json.Unmarshal(celeryMsg.Kwargs, &task); err != nil {
		return fmt.Errorf("failed to unmarshal update_space task kwargs: %w", err)
	}

	// Convert SpaceData to Space model for DB
	spaceData := task.Data

	c.logger.Info("Processing update_space task",
		zap.String("org", task.OrganizationSlugName),
		zap.String("space_slug", spaceData.SlugName),
		zap.String("space_id", spaceData.ID.String()))

	// Ensure org schema and tables exist before upserting
	if err := c.dbClient.CreateSchemaAndTables(ctx, task.OrganizationSlugName); err != nil {
		return fmt.Errorf("failed to ensure schema for org '%s': %w", task.OrganizationSlugName, err)
	}

	// Upsert the space
	if err := c.dbClient.UpsertSpace(ctx, task.OrganizationSlugName, spaceData); err != nil {
		return fmt.Errorf("failed to upsert space: %w", err)
	}

	return nil
}

// handleDeleteSpace handles the delete_space Celery task
func (c *TaskConsumer) handleDeleteSpace(ctx context.Context, body []byte) error {
	// Parse Celery message format: [args, kwargs, metadata]
	var celeryMsg models.CeleryMessage
	if err := json.Unmarshal(body, &celeryMsg); err != nil {
		return fmt.Errorf("failed to unmarshal celery message: %w", err)
	}

	var task models.DeleteSpaceTask
	if err := json.Unmarshal(celeryMsg.Kwargs, &task); err != nil {
		return fmt.Errorf("failed to unmarshal delete_space task kwargs: %w", err)
	}

	// Parse space ID
	spaceID, err := parseUUID(task.PK.String())
	if err != nil {
		return fmt.Errorf("invalid space ID '%s': %w", task.PK, err)
	}

	c.logger.Info("Processing delete_space task",
		zap.String("org", task.OrganizationSlugName),
		zap.String("space_id", spaceID.String()))

	// Delete the space
	if err := c.dbClient.DeleteSpace(ctx, task.OrganizationSlugName, spaceID); err != nil {
		return fmt.Errorf("failed to delete space: %w", err)
	}

	return nil
}

// handleDeleteDevice handles the delete_device Celery task
func (c *TaskConsumer) handleDeleteDevice(ctx context.Context, body []byte) error {
	var celeryMsg models.CeleryMessage
	if err := json.Unmarshal(body, &celeryMsg); err != nil {
		return fmt.Errorf("failed to unmarshal celery message: %w", err)
	}

	var task models.DeleteDeviceTask
	if err := json.Unmarshal(celeryMsg.Kwargs, &task); err != nil {
		return fmt.Errorf("failed to unmarshal delete_device task kwargs: %w", err)
	}

	deviceID, err := parseUUID(task.DeviceID)
	if err != nil {
		return fmt.Errorf("invalid device ID '%s': %w", task.DeviceID, err)
	}

	c.logger.Info("Processing delete_device task",
		zap.String("org", task.OrganizationSlugName),
		zap.String("device_id", deviceID.String()))

	if err := c.dbClient.DeleteDeviceFromSpace(ctx, task.OrganizationSlugName, deviceID); err != nil {
		return fmt.Errorf("failed to delete device telemetry data: %w", err)
	}

	return nil
}

// Stop gracefully stops the consumer
func (c *TaskConsumer) Stop() error {
	c.stopOnce.Do(func() {
		close(c.done)

		// Wait for processing to finish
		done := make(chan struct{})
		go func() {
			c.wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			c.logger.Info("Celery task consumer stopped gracefully")
		case <-time.After(5 * time.Second):
			c.logger.Warn("Celery task consumer stop timeout")
		}

		if c.channel != nil {
			if err := c.channel.Close(); err != nil {
				c.logger.Error("Failed to close channel", zap.Error(err))
			}
		}
		if c.conn != nil {
			if err := c.conn.Close(); err != nil {
				c.logger.Error("Failed to close connection", zap.Error(err))
			}
		}
	})
	return nil
}

// IsHealthy checks if the consumer is healthy
func (c *TaskConsumer) IsHealthy() bool {
	return c.conn != nil && !c.conn.IsClosed()
}

// parseUUID parses a UUID from string
func parseUUID(s string) (uuid.UUID, error) {
	return uuid.Parse(s)
}
