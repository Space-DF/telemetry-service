package events

import (
	"github.com/Space-DF/telemetry-service/internal/config"
	"github.com/Space-DF/telemetry-service/internal/timescaledb"
	"github.com/labstack/echo/v4"
	"go.uber.org/zap"
)

func RegisterRoutes(e *echo.Group, cfg *config.Config, logger *zap.Logger, tsClient *timescaledb.Client) {
	group := e.Group("/events")
	group.GET("", getDeviceStatus(logger, tsClient, cfg.DeviceStatus.Events))
}
