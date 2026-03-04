/*
Copyright 2026 Digital Fortress.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	alertregistry "github.com/Space-DF/telemetry-service/internal/alerts/registry"
	amqp "github.com/Space-DF/telemetry-service/internal/amqp/multi-tenant"
	celeryconsumer "github.com/Space-DF/telemetry-service/internal/celery"
	"github.com/Space-DF/telemetry-service/internal/api"
	"github.com/Space-DF/telemetry-service/internal/events/registry"
	"github.com/Space-DF/telemetry-service/internal/health"
	"github.com/Space-DF/telemetry-service/internal/services"
	"github.com/Space-DF/telemetry-service/internal/timescaledb"
	"github.com/Space-DF/telemetry-service/pkgs/db"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/urfave/cli/v2"
	"go.uber.org/zap"
)

const ShutdownTimeout = 30 * time.Second

func cmdServe(ctx *cli.Context, logger *zap.Logger) error {
	defer func() {
		if err := logger.Sync(); err != nil {
			logger.Debug("Error syncing logger", zap.Error(err))
		}
	}()

	logger.Info("Starting Telemetry Service",
		zap.String("version", "1.0.0"),
		zap.String("mode", "multi-tenant"),
		zap.Any("config", appConfig),
	)

	// Load alert processors from config (if provided)
	loadAlertProcessors(logger, appConfig.Server.AlertsProcessorsCfg)

	// Run database migrations
	logger.Info("Running database migrations...")
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable",
		appConfig.Db.Username, appConfig.Db.Password, appConfig.Db.Host, appConfig.Db.Port, appConfig.Db.Name)
	dbURL, err := url.Parse(dsn)
	if err != nil {
		return fmt.Errorf("failed to parse database DSN: %w", err)
	}

	migrationPath := "pkgs/db/migrations"
	if err := db.Migrate(dbURL, migrationPath); err != nil {
		return fmt.Errorf("failed to run migrations: %w", err)
	}
	logger.Info("Database migrations completed successfully")

	// Initialize Psql client
	tsClient, err := timescaledb.NewClient(
		dsn,
		appConfig.Db.BatchSize,
		appConfig.Db.FlushInterval,
		logger,
	)
	if err != nil {
		return fmt.Errorf("failed to initialize Psql client: %w", err)
	}
	defer func() {
		if err := tsClient.Close(); err != nil {
			logger.Error("Failed to close Psql client", zap.Error(err))
		}
	}()

	// Initialize rule registry
	ruleRegistry := registry.NewRuleRegistry(tsClient, logger)

	// Load default rules from YAML
	if appConfig.Server.EventRulesDir != "" {
		if err := ruleRegistry.LoadDefaultRulesFromDir(appConfig.Server.EventRulesDir); err != nil {
			logger.Warn("Failed to load default event rules", zap.Error(err))
		}
	}

	// Initialize location processor
	processor := services.NewLocationProcessor(tsClient, ruleRegistry, logger)

	// Initialize multi-tenant AMQP consumer with schema initializer
	consumer := amqp.NewMultiTenantConsumer(appConfig.AMQP, appConfig.OrgEvents, processor, tsClient, logger)

	// Connect to RabbitMQ
	if err := consumer.Connect(); err != nil {
		return fmt.Errorf("failed to connect to AMQP: %w", err)
	}

	// Initialize Celery task consumer for space synchronization
	celeryConsumer := celeryconsumer.NewTaskConsumer(appConfig.AMQP.BrokerURL, tsClient, logger)
	if err := celeryConsumer.Connect(); err != nil {
		logger.Warn("Failed to connect Celery task consumer (space sync will be unavailable)", zap.Error(err))
		// Don't fail startup - Celery consumer is optional
		celeryConsumer = nil
	} else {
		logger.Info("Celery task consumer connected for space synchronization")
	}

	// Initialize Echo
	e := echo.New()
	e.HideBanner = true
	e.HidePort = true

	// Middleware
	e.Use(middleware.Logger())
	e.Use(middleware.Recover())
	e.Use(middleware.CORS())

	group := e.Group("/api/telemetry")
	api.Setup(appConfig, group, logger, tsClient)
	health.Setup(group, consumer, tsClient, logger)

	// Create context for graceful shutdown
	srvCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start Echo server
	go func() {
		logger.Info("Starting API server", zap.Int("port", appConfig.Server.APIPort))
		addr := fmt.Sprintf(":%d", appConfig.Server.APIPort)
		if err := e.Start(addr); err != nil {
			logger.Error("API server error", zap.Error(err))
		}
	}()

	// Start AMQP consumer
	go func() {
		logger.Info("Starting AMQP consumer")
		if err := consumer.Start(srvCtx); err != nil {
			logger.Error("AMQP consumer error", zap.Error(err))
			cancel()
		}
	}()

	// Start Celery task consumer (if connected)
	if celeryConsumer != nil {
		go func() {
			logger.Info("Starting Celery task consumer")
			if err := celeryConsumer.Start(srvCtx); err != nil {
				logger.Error("Celery task consumer error", zap.Error(err))
				// Don't cancel context - Celery consumer is optional
			}
		}()
	}

	// Setup reload signal for alert processors and event rules
	reloadChan := make(chan os.Signal, 1)
	signal.Notify(reloadChan, syscall.SIGHUP)
	go func() {
		for range reloadChan {
			loadAlertProcessors(logger, appConfig.Server.AlertsProcessorsCfg)
			if appConfig.Server.EventRulesDir != "" {
				if err := ruleRegistry.ReloadDefaultRules(appConfig.Server.EventRulesDir); err != nil {
					logger.Warn("Failed to reload default event rules", zap.Error(err))
				}
			}
		}
	}()

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	logger.Info("Shutting down service...")

	// Cancel context to stop all components
	cancel()

	// Give components time to cleanup
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), ShutdownTimeout)
	defer shutdownCancel()

	// Stop Echo server
	if err := e.Shutdown(shutdownCtx); err != nil {
		logger.Error("Error shutting down API server", zap.Error(err))
	}

	// Stop AMQP consumer
	if err := consumer.Stop(); err != nil {
		logger.Error("Error stopping AMQP consumer", zap.Error(err))
	}

	// Stop Celery task consumer
	if celeryConsumer != nil {
		if err := celeryConsumer.Stop(); err != nil {
			logger.Error("Error stopping Celery task consumer", zap.Error(err))
		}
	}

	// Wait for batch writer to finish draining with timeout
	logger.Info("Waiting for batch writer to finish draining...")
	done := make(chan struct{})
	go func() {
		tsClient.Wait()
		close(done)
	}()

	select {
	case <-done:
		logger.Info("Batch writer finished draining successfully")
	case <-shutdownCtx.Done():
		logger.Warn("Batch writer drain timeout exceeded, some data may be lost")
	}

	logger.Info("Service shutdown complete")
	return nil
}

func loadAlertProcessors(logger *zap.Logger, path string) {
	if strings.TrimSpace(path) == "" {
		return
	}

	processors, err := alertregistry.LoadFromConfig(path)
	if err != nil {
		logger.Warn("Failed to load alert processors from config", zap.Error(err), zap.String("path", path))
		return
	}

	alertregistry.ReplaceAll(processors)
	logger.Info("Loaded alert processors from config", zap.String("path", path), zap.Int("count", len(processors)))
}
