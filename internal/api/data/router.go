package data

import (
	"github.com/Space-DF/telemetry-service/internal/timescaledb"
	"github.com/labstack/echo/v4"
	"go.uber.org/zap"
)

func RegisterRoutes(e *echo.Group, logger *zap.Logger, tsClient *timescaledb.Client) {
	group := e.Group("/data")
	group.GET("/latest", getDeviceProperties(logger, tsClient))
}
