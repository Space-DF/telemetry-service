package geofences

import (
	"github.com/Space-DF/telemetry-service/internal/config"
	"github.com/Space-DF/telemetry-service/internal/timescaledb"
	"github.com/labstack/echo/v4"
	"go.uber.org/zap"
)

func RegisterRoutes(e *echo.Group, cfg *config.Config, logger *zap.Logger, tsClient *timescaledb.Client) {
	// Geofences CRUD
	e.GET("/geofences", getGeofences(logger, tsClient))
	e.POST("/geofences", createGeofence(logger, tsClient))
	e.GET("/geofences/:geofence_id", getGeofenceByID(logger, tsClient))
	e.PUT("/geofences/:geofence_id", updateGeofence(logger, tsClient))
	e.DELETE("/geofences/:geofence_id", deleteGeofence(logger, tsClient))
	e.POST("/geofences/test", testGeofence(logger, tsClient))

	// Device-related geofence queries
	e.GET("/geofences/device/:device_id", getGeofencesByDevice(logger, tsClient))
}
