package widget

import (
	"github.com/Space-DF/telemetry-service/internal/timescaledb"
	"github.com/labstack/echo/v4"
	"go.uber.org/zap"
)

func RegisterRoutes(group *echo.Group, logger *zap.Logger, tsClient *timescaledb.Client) {
	group.GET("/widget/data/:entity_id", getWidgetData(logger, tsClient))
}
