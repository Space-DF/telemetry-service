package health

import (
	"github.com/Space-DF/telemetry-service/internal/timescaledb"
	"github.com/labstack/echo/v4"
	"go.uber.org/zap"
)

// HealthChecker interface for health checking
type HealthChecker interface {
	IsHealthy() bool
}

// Setup registers health check routes
func Setup(e *echo.Group, healthChecker HealthChecker, tsClient *timescaledb.Client, logger *zap.Logger) {
	e.GET("/health", handleHealth(healthChecker, tsClient, logger))
	e.GET("/ready", handleReady(healthChecker, tsClient, logger))
	e.GET("/live", handleLive(logger))
}
