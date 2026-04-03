package automations

import (
	"github.com/Space-DF/telemetry-service/internal/timescaledb"
	"github.com/labstack/echo/v4"
	"go.uber.org/zap"
)

func RegisterRoutes(group *echo.Group, logger *zap.Logger, tsClient *timescaledb.Client) {
	handler := NewHandler(logger, tsClient)

	// Automations CRUD routes
	group.GET("/automations/summary", handler.GetAutomationsSummary)
	group.GET("/automations", handler.GetAutomations)
	group.GET("/automations/:automation_id", handler.GetAutomationByID)
	group.POST("/automations", handler.CreateAutomation)
	group.PUT("/automations/:automation_id", handler.UpdateAutomation)
	group.DELETE("/automations/:automation_id", handler.DeleteAutomation)

	// Actions CRUD routes
	group.GET("/actions", handler.GetActions)
	group.POST("/actions", handler.CreateAction)
	group.PUT("/actions/:action_id", handler.UpdateAction)
	group.DELETE("/actions/:action_id", handler.DeleteAction)
}
