package events

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/Space-DF/telemetry-service/internal/api/common"
	"github.com/Space-DF/telemetry-service/internal/models"
	"github.com/Space-DF/telemetry-service/internal/timescaledb"
	"github.com/labstack/echo/v4"
	"go.uber.org/zap"
)

// ============================================================================
// Events API Handlers
// ============================================================================

// getEventsByEntity returns all events for a specific entity
func getEventsByEntity(logger *zap.Logger, tsClient *timescaledb.Client) echo.HandlerFunc {
	return func(c echo.Context) error {
		orgToUse := common.ResolveOrgFromRequest(c)
		if orgToUse == "" {
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": "Could not determine organization from hostname or X-Organization header",
			})
		}

		entityID := strings.TrimSpace(c.Param("entity_id"))
		if entityID == "" {
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": "entity_id is required",
			})
		}

		limit := 100
		if limitStr := c.QueryParam("limit"); limitStr != "" {
			if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
				limit = l
			}
		}

		ctx := timescaledb.ContextWithOrg(c.Request().Context(), orgToUse)
		events, err := tsClient.GetEventsByEntity(ctx, orgToUse, entityID, limit)
		if err != nil {
			logger.Error("failed to get events by entity",
				zap.String("entity_id", entityID),
				zap.Error(err))
			return c.JSON(http.StatusInternalServerError, map[string]string{
				"error": "failed to get events",
			})
		}

		return c.JSON(http.StatusOK, map[string]interface{}{
			"entity_id": entityID,
			"events":    events,
			"count":     len(events),
		})
	}
}

// ============================================================================
// Level Events API Handlers
// ============================================================================

// getLevelEvents returns all level events
func getLevelEvents(logger *zap.Logger, tsClient *timescaledb.Client) echo.HandlerFunc {
	return func(c echo.Context) error {
		orgToUse := common.ResolveOrgFromRequest(c)
		if orgToUse == "" {
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": "Could not determine organization from hostname or X-Organization header",
			})
		}

		page := 1
		if pageStr := c.QueryParam("page"); pageStr != "" {
			if p, err := strconv.Atoi(pageStr); err == nil && p > 0 {
				page = p
			}
		}

		pageSize := 20
		if sizeStr := c.QueryParam("page_size"); sizeStr != "" {
			if s, err := strconv.Atoi(sizeStr); err == nil && s > 0 && s <= 100 {
				pageSize = s
			}
		}

		ownerType := c.QueryParam("owner_type")

		ctx := timescaledb.ContextWithOrg(c.Request().Context(), orgToUse)
		events, total, err := tsClient.GetLevelEvents(ctx, ownerType, page, pageSize)
		if err != nil {
			logger.Error("failed to get level events",
				zap.Error(err))
			return c.JSON(http.StatusInternalServerError, map[string]string{
				"error": "failed to get level events",
			})
		}

		return c.JSON(http.StatusOK, models.LevelEventsListResponse{
			Events:     events,
			TotalCount: total,
			Page:       page,
			PageSize:   pageSize,
		})
	}
}

// createLevelEvent creates a new level event
func createLevelEvent(logger *zap.Logger, tsClient *timescaledb.Client) echo.HandlerFunc {
	return func(c echo.Context) error {
		orgToUse := common.ResolveOrgFromRequest(c)
		if orgToUse == "" {
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": "Could not determine organization from hostname or X-Organization header",
			})
		}

		var req models.LevelEventRequest
		if err := c.Bind(&req); err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": "invalid request body",
			})
		}

		ctx := timescaledb.ContextWithOrg(c.Request().Context(), orgToUse)
		event, err := tsClient.CreateLevelEvent(ctx, &req)
		if err != nil {
			logger.Error("failed to create level event",
				zap.Error(err))
			return c.JSON(http.StatusInternalServerError, map[string]string{
				"error": "failed to create level event",
			})
		}

		return c.JSON(http.StatusCreated, event)
	}
}

// ============================================================================
// Event Rules API Handlers
// ============================================================================

// getEventRules returns all event rules
func getEventRules(logger *zap.Logger, tsClient *timescaledb.Client) echo.HandlerFunc {
	return func(c echo.Context) error {
		orgToUse := common.ResolveOrgFromRequest(c)
		if orgToUse == "" {
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": "Could not determine organization from hostname or X-Organization header",
			})
		}

		page := 1
		if pageStr := c.QueryParam("page"); pageStr != "" {
			if p, err := strconv.Atoi(pageStr); err == nil && p > 0 {
				page = p
			}
		}

		pageSize := 20
		if sizeStr := c.QueryParam("page_size"); sizeStr != "" {
			if s, err := strconv.Atoi(sizeStr); err == nil && s > 0 && s <= 100 {
				pageSize = s
			}
		}

		levelEventID := c.QueryParam("level_event_id")
		entityID := c.QueryParam("entity_id")

		ctx := timescaledb.ContextWithOrg(c.Request().Context(), orgToUse)
		rules, total, err := tsClient.GetEventRules(ctx, levelEventID, entityID, page, pageSize)
		if err != nil {
			logger.Error("failed to get event rules",
				zap.Error(err))
			return c.JSON(http.StatusInternalServerError, map[string]string{
				"error": "failed to get event rules",
			})
		}

		return c.JSON(http.StatusOK, models.EventRulesListResponse{
			Rules:      rules,
			TotalCount: total,
			Page:       page,
			PageSize:   pageSize,
		})
	}
}

// createEventRule creates a new event rule
func createEventRule(logger *zap.Logger, tsClient *timescaledb.Client) echo.HandlerFunc {
	return func(c echo.Context) error {
		orgToUse := common.ResolveOrgFromRequest(c)
		if orgToUse == "" {
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": "Could not determine organization from hostname or X-Organization header",
			})
		}

		var req models.EventRuleRequest
		if err := c.Bind(&req); err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": "invalid request body",
			})
		}

		ctx := timescaledb.ContextWithOrg(c.Request().Context(), orgToUse)
		rule, err := tsClient.CreateEventRule(ctx, &req)
		if err != nil {
			logger.Error("failed to create event rule",
				zap.Error(err))
			return c.JSON(http.StatusInternalServerError, map[string]string{
				"error": "failed to create event rule",
			})
		}

		return c.JSON(http.StatusCreated, rule)
	}
}

// updateEventRule updates an existing event rule
func updateEventRule(logger *zap.Logger, tsClient *timescaledb.Client) echo.HandlerFunc {
	return func(c echo.Context) error {
		orgToUse := common.ResolveOrgFromRequest(c)
		if orgToUse == "" {
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": "Could not determine organization from hostname or X-Organization header",
			})
		}

		ruleID := strings.TrimSpace(c.Param("rule_id"))
		if ruleID == "" {
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": "rule_id is required",
			})
		}

		var req models.EventRuleRequest
		if err := c.Bind(&req); err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": "invalid request body",
			})
		}

		ctx := timescaledb.ContextWithOrg(c.Request().Context(), orgToUse)
		rule, err := tsClient.UpdateEventRule(ctx, ruleID, &req)
		if err != nil {
			logger.Error("failed to update event rule",
				zap.String("rule_id", ruleID),
				zap.Error(err))
			return c.JSON(http.StatusInternalServerError, map[string]string{
				"error": "failed to update event rule",
			})
		}

		return c.JSON(http.StatusOK, rule)
	}
}

// deleteEventRule deletes an event rule
func deleteEventRule(logger *zap.Logger, tsClient *timescaledb.Client) echo.HandlerFunc {
	return func(c echo.Context) error {
		orgToUse := common.ResolveOrgFromRequest(c)
		if orgToUse == "" {
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": "Could not determine organization from hostname or X-Organization header",
			})
		}

		ruleID := strings.TrimSpace(c.Param("rule_id"))
		if ruleID == "" {
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": "rule_id is required",
			})
		}

		ctx := timescaledb.ContextWithOrg(c.Request().Context(), orgToUse)
		if err := tsClient.DeleteEventRule(ctx, ruleID); err != nil {
			logger.Error("failed to delete event rule",
				zap.String("rule_id", ruleID),
				zap.Error(err))
			return c.JSON(http.StatusInternalServerError, map[string]string{
				"error": "failed to delete event rule",
			})
		}

		return c.JSON(http.StatusOK, map[string]string{
			"message": "event rule deleted successfully",
		})
	}
}

// ============================================================================
// States API Handlers
// ============================================================================

// recordStateChange records a state change event and creates a new state record
func recordStateChange(logger *zap.Logger, tsClient *timescaledb.Client) echo.HandlerFunc {
	return func(c echo.Context) error {
		orgToUse := common.ResolveOrgFromRequest(c)
		if orgToUse == "" {
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": "Could not determine organization from hostname or X-Organization header",
			})
		}

		spaceSlug, err := common.ResolveSpaceSlugFromRequest(c)
		if err != nil {
			return err
		}

		var req models.StateChangeRequest
		if err := c.Bind(&req); err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": "invalid request body",
			})
		}

		if req.EntityID == "" {
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": "entity_id is required",
			})
		}
		if req.NewState == "" {
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": "new_state is required",
			})
		}

		ctx := timescaledb.ContextWithOrg(c.Request().Context(), orgToUse)
		state, err := tsClient.RecordStateChange(ctx, orgToUse, spaceSlug, &req)
		if err != nil {
			logger.Error("failed to record state change",
				zap.String("entity_id", req.EntityID),
				zap.Error(err))
			return c.JSON(http.StatusInternalServerError, map[string]string{
				"error": "failed to record state change",
			})
		}

		return c.JSON(http.StatusCreated, state)
	}
}
