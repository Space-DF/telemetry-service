package events

import (
	"github.com/Space-DF/telemetry-service/internal/config"
	"github.com/Space-DF/telemetry-service/internal/timescaledb"
	"github.com/labstack/echo/v4"
	"go.uber.org/zap"
)

func RegisterRoutes(e *echo.Group, cfg *config.Config, logger *zap.Logger, tsClient *timescaledb.Client) {
	// Get events for a specific entity
	e.GET("/events/entity/:entity_id", getEventsByEntity(logger, tsClient))

	// Get level events
	e.GET("/level-events", getLevelEvents(logger, tsClient))
	// Create a new level event
	e.POST("/level-events", createLevelEvent(logger, tsClient))

	// Get event rules
	e.GET("/event-rules", getEventRules(logger, tsClient))
	// Create a new event rule
	e.POST("/event-rules", createEventRule(logger, tsClient))
	// Update an event rule
	e.PUT("/event-rules/:rule_id", updateEventRule(logger, tsClient))
	// Delete an event rule
	e.DELETE("/event-rules/:rule_id", deleteEventRule(logger, tsClient))

	// Record a state change
	e.POST("/states", recordStateChange(logger, tsClient))
}
