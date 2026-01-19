package entities

import (
	"github.com/Space-DF/telemetry-service/internal/timescaledb"
	"github.com/labstack/echo/v4"
	"go.uber.org/zap"
)

func RegisterRoutes(e *echo.Group, logger *zap.Logger, tsClient *timescaledb.Client) {
	group := e.Group("/entities")
	group.GET("", getEntities(logger, tsClient))

	// Update entity trigger event configuration
	group.PUT("/:entity_id/trigger-event", updateEntityTriggerEvent(logger, tsClient))
}
