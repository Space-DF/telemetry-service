package notifications

import (
	"github.com/Space-DF/telemetry-service/internal/api/middleware"
	"github.com/Space-DF/telemetry-service/internal/timescaledb"
	"github.com/labstack/echo/v4"
	"go.uber.org/zap"
)

// RegisterRoutes registers notification-related API routes
func RegisterRoutes(
	e *echo.Group,
	logger *zap.Logger,
	tsClient *timescaledb.Client,
	vapidPublic string,
) {
	group := e.Group("/notifications")
	group.Use(middleware.UserIDFromHeader())
	group.POST("/subscribe", SubscribeHandler(logger, tsClient))
	group.DELETE("/subscribe/:id", UnsubscribeHandler(logger, tsClient))
}
