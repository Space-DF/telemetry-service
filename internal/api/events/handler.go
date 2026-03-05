package events

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/Space-DF/telemetry-service/internal/api/common"
	apimodels "github.com/Space-DF/telemetry-service/internal/api/events/models"
	"github.com/Space-DF/telemetry-service/internal/models"
	"github.com/Space-DF/telemetry-service/internal/timescaledb"
	"github.com/labstack/echo/v4"
	"go.uber.org/zap"
)

// getEventsByDevice returns all events for a specific device
// @Summary Get events by device
// @Description Retrieve all events for a specific device with optional time range filtering. Organization is resolved from X-Organization header or hostname (e.g., {org}.localhost)
// @Tags events
// @Accept json
// @Produce json
// @Param device_id path string true "Device ID"
// @Param limit query int false "Maximum number of events to return (default 100)"
// @Param start_time query int64 false "Start time as Unix timestamp (milliseconds)"
// @Param end_time query int64 false "End time as Unix timestamp (milliseconds)"
// @Success 200 {object} models.EventsByDeviceResponse
// @Failure 400 {object} models.ErrorResponse "Invalid request parameters"
// @Failure 500 {object} models.ErrorResponse "Internal server error"
// @Router /telemetry/v1/events/device/{device_id} [get]
func getEventsByDevice(logger *zap.Logger, tsClient *timescaledb.Client) echo.HandlerFunc {
	return func(c echo.Context) error {
		orgToUse := common.ResolveOrgFromRequest(c)
		if orgToUse == "" {
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": "Could not determine organization from hostname or X-Organization header",
			})
		}

		req := &apimodels.EventsByDeviceRequest{
			DeviceID: strings.TrimSpace(c.Param("device_id")),
		}

		if req.DeviceID == "" {
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": "device_id is required",
			})
		}

		if limitStr := c.QueryParam("limit"); limitStr != "" {
			if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
				req.Limit = l
			}
		}

		// Parse start_time and end_time query parameters
		if startTimeStr := c.QueryParam("start_time"); startTimeStr != "" {
			if ms, err := strconv.ParseInt(startTimeStr, 10, 64); err == nil {
				req.StartTime = &ms
			}
		}
		if endTimeStr := c.QueryParam("end_time"); endTimeStr != "" {
			if ms, err := strconv.ParseInt(endTimeStr, 10, 64); err == nil {
				req.EndTime = &ms
			}
		}
		req.SetDefaults()

		ctx := timescaledb.ContextWithOrg(c.Request().Context(), orgToUse)
		events, err := tsClient.GetEventsByDevice(ctx, orgToUse, req.DeviceID, req.Limit, req.StartTime, req.EndTime)
		if err != nil {
			logger.Error("failed to get events by device",
				zap.String("device_id", req.DeviceID),
				zap.Error(err))
			return c.JSON(http.StatusInternalServerError, map[string]string{
				"error": "failed to get events",
			})
		}

		return c.JSON(http.StatusOK, apimodels.EventsByDeviceResponse{
			DeviceID: req.DeviceID,
			Events:   convertEventsToItems(events),
			Count:    len(events),
		})
	}
}

// getEventRules returns all event rules
// @Summary Get event rules
// @Description Retrieve all event rules with optional filtering by device. Organization is resolved from X-Organization header or hostname (e.g., {org}.localhost)
// @Tags events
// @Accept json
// @Produce json
// @Param device_id query string false "Filter by device ID"
// @Param page query int false "Page number (default 1)"
// @Param page_size query int false "Page size (default 20)"
// @Success 200 {object} models.EventRulesResponse
// @Failure 400 {object} models.ErrorResponse "Invalid request parameters"
// @Failure 500 {object} models.ErrorResponse "Internal server error"
// @Router /telemetry/v1/event-rules [get]
func getEventRules(logger *zap.Logger, tsClient *timescaledb.Client) echo.HandlerFunc {
	return func(c echo.Context) error {
		orgToUse := common.ResolveOrgFromRequest(c)
		if orgToUse == "" {
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": "Could not determine organization from hostname or X-Organization header",
			})
		}

		req := &apimodels.EventRulesRequest{
			DeviceID: c.QueryParam("device_id"),
		}
		if pageStr := c.QueryParam("page"); pageStr != "" {
			if p, err := strconv.Atoi(pageStr); err == nil && p > 0 {
				req.Page = p
			}
		}
		if sizeStr := c.QueryParam("page_size"); sizeStr != "" {
			if s, err := strconv.Atoi(sizeStr); err == nil && s > 0 {
				req.PageSize = s
			}
		}
		req.SetDefaults()

		ctx := timescaledb.ContextWithOrg(c.Request().Context(), orgToUse)
		rules, total, err := tsClient.GetEventRules(ctx, req.DeviceID, req.Page, req.PageSize)
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
			Page:       req.Page,
			PageSize:   req.PageSize,
		})
	}
}

// createEventRule creates a new event rule
// @Summary Create event rule
// @Description Create a new event rule for monitoring and alerting. Organization is resolved from X-Organization header or hostname (e.g., {org}.localhost)
// @Tags events
// @Accept json
// @Produce json
// @Param request body models.EventRuleRequest true "Event rule configuration"
// @Success 201 {object} models.EventRuleItem
// @Failure 400 {object} models.ErrorResponse "Invalid request parameters"
// @Failure 500 {object} models.ErrorResponse "Internal server error"
// @Router /telemetry/v1/event-rules [post]
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
// @Summary Update event rule
// @Description Update an existing event rule by ID. Organization is resolved from X-Organization header or hostname (e.g., {org}.localhost)
// @Tags events
// @Accept json
// @Produce json
// @Param rule_id path string true "Event Rule ID"
// @Param request body models.EventRuleRequest true "Event rule configuration"
// @Success 200 {object} models.EventRuleItem
// @Failure 400 {object} models.ErrorResponse "Invalid request parameters"
// @Failure 500 {object} models.ErrorResponse "Internal server error"
// @Router /telemetry/v1/event-rules/{rule_id} [put]
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
// @Summary Delete event rule
// @Description Delete an event rule by ID. Organization is resolved from X-Organization header or hostname (e.g., {org}.localhost)
// @Tags events
// @Accept json
// @Produce json
// @Param rule_id path string true "Event Rule ID"
// @Success 200 {object} models.DeleteEventRuleResponse
// @Failure 400 {object} models.ErrorResponse "Invalid request parameters"
// @Failure 500 {object} models.ErrorResponse "Internal server error"
// @Router /telemetry/v1/event-rules/{rule_id} [delete]
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

		return c.JSON(http.StatusOK, apimodels.DeleteEventRuleResponse{
			Message: "event rule deleted successfully",
		})
	}
}

// convertEventsToItems converts internal models.Event to API response items
func convertEventsToItems(events []models.Event) []apimodels.EventItem {
	items := make([]apimodels.EventItem, len(events))
	for i, e := range events {
		items[i] = apimodels.EventItem{
			EventID:    e.EventID,
			EventType:  e.EventType,
			EventLevel: safeString(e.EventLevel),
			SpaceSlug:  e.SpaceSlug,
			EntityID:   safeString(e.EntityID),
			TimeFired:  e.TimeFired(),
		}
		if e.SharedData != nil {
			data, _ := e.ParseEventData()
			items[i].EventData = data
		}
	}
	return items
}

func safeString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}