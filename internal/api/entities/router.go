package entities

import (
	"github.com/Space-DF/telemetry-service/internal/timescaledb"
	"github.com/labstack/echo/v4"
	"go.uber.org/zap"
)

func RegisterRoutes(e *echo.Group, logger *zap.Logger, tsClient *timescaledb.Client) {
	group := e.Group("/entities")
	group.GET("", getEntities(logger, tsClient))

	// Update device trigger event configuration
	group.PUT("/:device_id/trigger-event", updateDeviceTriggerEvent(logger, tsClient))
}
