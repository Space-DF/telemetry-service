package alerts

import (
	"github.com/Space-DF/telemetry-service/internal/timescaledb"
	"github.com/labstack/echo/v4"
	"go.uber.org/zap"
)

func RegisterRoutes(group *echo.Group, logger *zap.Logger, tsClient *timescaledb.Client) {
	handler := NewHandler(logger, tsClient)

	group.GET("/alerts", handler.GetAlerts)
}
