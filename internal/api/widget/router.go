package widget

import (
	"github.com/Space-DF/telemetry-service/internal/timescaledb"
	"github.com/labstack/echo/v4"
	"go.uber.org/zap"
)

func RegisterRoutes(e *echo.Group, logger *zap.Logger, tsClient *timescaledb.Client) {
	group := e.Group("/widget")
	group.GET("/data/:entity_id", getWidgetData(logger, tsClient))
}
