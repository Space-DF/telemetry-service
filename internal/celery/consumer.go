package celery

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/Space-DF/telemetry-service/internal/celery/subscription"
	"github.com/Space-DF/telemetry-service/internal/celery/taskerrors"
	"github.com/Space-DF/telemetry-service/internal/celery/topology"
	"github.com/Space-DF/telemetry-service/internal/client"
	"github.com/Space-DF/telemetry-service/internal/models"
	"github.com/Space-DF/telemetry-service/internal/timescaledb"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

const (
	// Task names for message identification
	UpdateSpaceTask          = "update_space"
	DeleteSpaceTask          = "delete_space"
	DeleteDeviceTask         = "delete_device"
	CreateDeviceEntitiesTask = "create_device_entities"
)

// TaskConsumer consumes Celery tasks from RabbitMQ
type TaskConsumer struct {
	amqpURL           string
	dbClient          *timescaledb.Client
	logger            *zap.Logger
	transformerClient *client.TransformerServiceClient
	conn              *amqp.Connection
	channel           *amqp.Channel
	done              chan bool
	wg                sync.WaitGroup
	stopOnce          sync.Once
	taskHandlers      map[string]TaskHandler
	specs             []topology.Spec
}

// NewTaskConsumer creates a new Celery task consumer
func NewTaskConsumer(amqpURL string, dbClient *timescaledb.Client, logger *zap.Logger) (*TaskConsumer, error) {
	subscriptionHandler := subscription.NewHandler(dbClient, logger)
	consumer := &TaskConsumer{
		amqpURL:           amqpURL,
		dbClient:          dbClient,
		logger:            logger,
		transformerClient: client.NewTransformerServiceClient(logger),
		done:              make(chan bool, 1),
	}

	handlers := []QueueBoundHandler{
		newFanoutTaskHandler(
			UpdateSpaceTask,
			consumer.handleUpdateSpace,
		),
		newFanoutTaskHandler(
			DeleteSpaceTask,
			consumer.handleDeleteSpace,
		),
		newFanoutTaskHandler(
			DeleteDeviceTask,
			consumer.handleDeleteDevice,
		),
		newFanoutTaskHandler(
			CreateDeviceEntitiesTask,
			consumer.handleCreateDeviceEntities,
		),
		subscriptionHandler,
	}

	taskHandlers, err := buildTaskHandlerRegistry(handlers...)
	if err != nil {
		return nil, fmt.Errorf("failed to build celery task handler registry: %w", err)
	}
	consumer.taskHandlers = taskHandlers
	consumer.specs = collectQueueSpecs(handlers...)
	return consumer, nil
}

func newFanoutTaskHandler(task string, handle func(context.Context, []byte) error) taskFuncHandler {
	taskName := "spacedf.tasks." + task
	queueName := "telemetry_" + task
	return taskFuncHandler{
		taskNames: []string{taskName, task},
		specs: []topology.Spec{
			{
				Exchange:     task,
				ExchangeType: "fanout",
				Queue:        queueName,
				RoutingKey:   task,
				ConsumerTag:  queueName + "_consumer",
				TaskName:     taskName,
			},
		},
		handle: func(ctx context.Context, _ string, body []byte) error {
			return handle(ctx, body)
		},
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
		c.tearDown()
		return fmt.Errorf("failed to set QoS: %w", err)
	}

	// Declare exchanges, queues and bindings from the topology table.
	for _, s := range c.specs {
		if err := c.declareTopology(s); err != nil {
			return err
		}
	}

	c.logger.Info("Celery task consumer connected",
		zap.Int("queues", len(c.specs)))

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

// connectAndConsume sets up consumption on the current connection.
//
// All Consume calls are issued first, then goroutines are spawned. If a
// Consume fails midway, the function returns and Start()'s reconnect path
// tears down the channel; the broker drops any consumers registered before
// the failure, so the state self-heals on the next connect cycle.
func (c *TaskConsumer) connectAndConsume(ctx context.Context) error {
	type queueConsumer struct {
		messages <-chan amqp.Delivery
		spec     topology.Spec
	}
	consumers := make([]queueConsumer, 0, len(c.specs))

	for _, s := range c.specs {
		messages, err := c.channel.Consume(
			s.Queue,
			s.ConsumerTag,
			false, // autoAck
			false, // exclusive
			false, // noLocal
			false, // noWait
			nil,
		)
		if err != nil {
			return fmt.Errorf("failed to start consuming queue %s: %w", s.Queue, err)
		}
		consumers = append(consumers, queueConsumer{messages: messages, spec: s})
	}

	c.logger.Info("Celery task consumer started", zap.Int("queues", len(c.specs)))

	// Spawn goroutines only after every Consume succeeded, so the WaitGroup
	// count always matches the number of running goroutines
	c.wg.Add(len(consumers))
	for _, qc := range consumers {
		go func(qc queueConsumer) {
			defer c.wg.Done()
			c.processMessages(ctx, qc.messages, qc.spec.TaskName)
		}(qc)
	}

	// Wait for all goroutines to finish (they exit when their channel closes)
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
				if taskerrors.IsPermanent(err) {
					c.logger.Warn("Rejecting Celery task without requeue",
						zap.String("task", expectedTaskName),
						zap.Error(err))
					_ = msg.Reject(false)
					continue
				}
				// Negative ack - requeue so transient failures are retried.
				_ = msg.Nack(false, true)
			} else {
				_ = msg.Ack(false)
			}
		}
	}
}

// handleTask processes a single Celery task
func (c *TaskConsumer) handleTask(ctx context.Context, taskName string, body []byte) error {
	handler, ok := c.taskHandlers[taskName]
	if !ok {
		return taskerrors.NewPermanentf("unsupported celery task: %s", taskName)
	}
	return handler.Handle(ctx, taskName, body)
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

func (c *TaskConsumer) handleCreateDeviceEntities(ctx context.Context, body []byte) error {
	var celeryMsg models.CeleryMessage
	if err := json.Unmarshal(body, &celeryMsg); err != nil {
		return fmt.Errorf("failed to unmarshal celery message: %w", err)
	}

	var task models.CreateDeviceEntitiesTask
	if err := json.Unmarshal(celeryMsg.Kwargs, &task); err != nil {
		return fmt.Errorf("failed to unmarshal create_device_entities task kwargs: %w", err)
	}

	c.logger.Info("Processing create_device_entities task",
		zap.String("org", task.OrganizationSlugName),
		zap.String("space_slug", task.SpaceSlug),
		zap.String("device_id", task.DeviceID),
		zap.String("device_model", task.DeviceModel),
		zap.String("dev_eui", task.DevEUI),
	)

	taskCtx := timescaledb.ContextWithOrg(ctx, task.OrganizationSlugName)

	templates, err := c.transformerClient.GetDeviceEntityTemplates(
		taskCtx,
		task.DeviceModel,
	)
	if err != nil {
		return fmt.Errorf("failed to fetch entity templates from transformer-service: %w", err)
	}

	createdCount, err := c.dbClient.CreateDeviceEntities(
		taskCtx,
		task.DeviceID,
		task.SpaceSlug,
		task.DeviceModel,
		task.DevEUI,
		templates,
	)
	if err != nil {
		return fmt.Errorf("failed to create device entities: %w", err)
	}

	c.logger.Info("Created device entities successfully",
		zap.String("org", task.OrganizationSlugName),
		zap.String("device_id", task.DeviceID),
		zap.Int64("created_count", createdCount),
	)

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
