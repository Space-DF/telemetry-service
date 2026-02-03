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

// getEventsByDevice returns all events for a specific device
func getEventsByDevice(logger *zap.Logger, tsClient *timescaledb.Client) echo.HandlerFunc {
	return func(c echo.Context) error {
		orgToUse := common.ResolveOrgFromRequest(c)
		if orgToUse == "" {
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": "Could not determine organization from hostname or X-Organization header",
			})
		}

		deviceID := strings.TrimSpace(c.Param("device_id"))
		if deviceID == "" {
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": "device_id is required",
			})
		}

		limit := 100
		if limitStr := c.QueryParam("limit"); limitStr != "" {
			if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
				limit = l
			}
		}

		// Parse start_time and end_time query parameters
		var startTime, endTime *int64
		if startTimeStr := c.QueryParam("start_time"); startTimeStr != "" {
			if ms, err := strconv.ParseInt(startTimeStr, 10, 64); err == nil {
				startTime = &ms
			}
		}
		if endTimeStr := c.QueryParam("end_time"); endTimeStr != "" {
			if ms, err := strconv.ParseInt(endTimeStr, 10, 64); err == nil {
				endTime = &ms
			}
		}

		ctx := timescaledb.ContextWithOrg(c.Request().Context(), orgToUse)
		events, err := tsClient.GetEventsByDevice(ctx, orgToUse, deviceID, limit, startTime, endTime)
		if err != nil {
			logger.Error("failed to get events by device",
				zap.String("device_id", deviceID),
				zap.Error(err))
			return c.JSON(http.StatusInternalServerError, map[string]string{
				"error": "failed to get events",
			})
		}

		return c.JSON(http.StatusOK, map[string]interface{}{
			"device_id": deviceID,
			"events":    events,
			"count":     len(events),
		})
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

		deviceID := c.QueryParam("device_id")

		ctx := timescaledb.ContextWithOrg(c.Request().Context(), orgToUse)
		rules, total, err := tsClient.GetEventRules(ctx, deviceID, page, pageSize)
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