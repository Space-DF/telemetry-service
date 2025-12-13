package api

import (
	"github.com/Space-DF/telemetry-service/internal/api/data"
	"github.com/Space-DF/telemetry-service/internal/api/entities"
	"github.com/Space-DF/telemetry-service/internal/api/location"
	"github.com/Space-DF/telemetry-service/internal/api/widget"
	"github.com/Space-DF/telemetry-service/internal/config"
	"github.com/Space-DF/telemetry-service/internal/timescaledb"
	"github.com/labstack/echo/v4"
	"go.uber.org/zap"
)

func Setup(cfg *config.Config, e *echo.Group, logger *zap.Logger, tsClient *timescaledb.Client) {
	group := e.Group("/v1")
	location.RegisterRoutes(group, logger, tsClient)
	entities.RegisterRoutes(group, logger, tsClient)
	widget.RegisterRoutes(group, logger, tsClient)
	data.RegisterRoutes(group, logger, tsClient)
}
