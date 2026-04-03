package automations

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/Space-DF/telemetry-service/internal/api/common"
	"github.com/Space-DF/telemetry-service/internal/client"

	apimodels "github.com/Space-DF/telemetry-service/internal/api/automations/models"
	"github.com/Space-DF/telemetry-service/internal/models"
	"github.com/Space-DF/telemetry-service/internal/timescaledb"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"go.uber.org/zap"
)

type Handler struct {
	logger              *zap.Logger
	tsClient            *timescaledb.Client
	deviceServiceClient *client.DeviceServiceClient
}

func NewHandler(logger *zap.Logger, tsClient *timescaledb.Client) *Handler {
	return &Handler{
		logger:              logger,
		tsClient:            tsClient,
		deviceServiceClient: client.NewDeviceServiceClient(logger),
	}
}

// GetAutomations returns automations with pagination, search, and filters
// @Summary Get automations
// @Description Retrieve automations with optional filtering by device_id, space_slug, and search. Organization is resolved from X-Organization header or hostname (e.g., {org}.localhost)
// @Tags automations
// @Accept json
// @Produce json
// @Param device_id query string false "Filter by device ID"
// @Param search query string false "Search by name or device ID"
// @Param limit query int false "Number of results per page (default 20)"
// @Param offset query int false "Number of results to skip (default 0)"
// @Success 200 {object} common.PaginatedResponse
// @Failure 400 {object} map[string]string "Invalid request parameters"
// @Failure 500 {object} map[string]string "Internal server error"
// @Router /telemetry/v1/automations [get]
func (h *Handler) GetAutomations(c echo.Context) error {
	org := common.ResolveOrgFromRequest(c)
	if org == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "Could not determine organization from hostname or X-Organization header",
		})
	}

	// Resolve space_id from X-Space header
	spaceSlug, err := common.ResolveSpaceSlugFromRequest(c)
	if err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "X-Space header is required",
		})
	}

	spaceID, err := h.tsClient.GetSpaceIDBySlug(c.Request().Context(), org, spaceSlug)
	if err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": fmt.Sprintf("Space '%s' not found", spaceSlug),
		})
	}

	// Parse query parameters
	var deviceID *string

	if deviceIDStr := strings.TrimSpace(c.QueryParam("device_id")); deviceIDStr != "" {
		if _, err := uuid.Parse(deviceIDStr); err == nil {
			deviceID = &deviceIDStr
		}
	}

	// Parse status query parameter (comma-separated booleans, e.g. "true,false")
	var statusList []bool
	if statusStr := strings.TrimSpace(c.QueryParam("status")); statusStr != "" {
		for _, s := range strings.Split(statusStr, ",") {
			trimmedS := strings.TrimSpace(s)
			if trimmedS == "" {
				continue
			}
			b, err := strconv.ParseBool(trimmedS)
			if err != nil {
				return c.JSON(http.StatusBadRequest, map[string]string{
					"error": fmt.Sprintf("invalid status value: '%s'", s),
				})
			}
			statusList = append(statusList, b)
		}
	}

	search := strings.TrimSpace(c.QueryParam("search"))

	// Pagination
	p := common.ParsePagination(c)

	ctx := timescaledb.ContextWithOrg(c.Request().Context(), org)

	automations, totalCount, err := h.tsClient.GetAutomations(ctx, spaceID, deviceID, statusList, search, p.Limit, p.Offset)
	if err != nil {
		h.logger.Error("failed to get automations", zap.Error(err))
		return c.JSON(http.StatusInternalServerError, map[string]string{
			"error": "failed to get automations",
		})
	}

	// Collect unique (deviceID, spaceID) pairs to fetch device space info concurrently.
	type deviceKey struct{ deviceID, spaceID string }
	uniqueKeys := make(map[deviceKey]struct{})
	for _, a := range automations {
		if a.DeviceID != "" && a.SpaceID != nil {
			uniqueKeys[deviceKey{a.DeviceID, a.SpaceID.String()}] = struct{}{}
		}
	}

	deviceSpaceCache := make(map[string]*client.DeviceSpaceInfo, len(uniqueKeys))
	var cacheMu sync.Mutex
	var wg sync.WaitGroup
	for dk := range uniqueKeys {
		wg.Add(1)
		go func(deviceID, spaceID string) {
			defer wg.Done()
			info, err := h.deviceServiceClient.GetDeviceSpaceByDeviceID(c.Request().Context(), deviceID, org, spaceID)
			if err != nil {
				h.logger.Warn("failed to fetch device space info", zap.String("device_id", deviceID), zap.Error(err))
				return
			}
			cacheMu.Lock()
			deviceSpaceCache[deviceID] = info
			cacheMu.Unlock()
		}(dk.deviceID, dk.spaceID)
	}
	wg.Wait()

	// Convert to response format using the pre-fetched cache.
	results := make([]map[string]interface{}, len(automations))
	for i, a := range automations {
		result := convertAutomationToMap(&a)

		if a.DeviceID == "" || a.SpaceID == nil {
			results[i] = result
			continue
		}

		info, ok := deviceSpaceCache[a.DeviceID]
		if !ok || info == nil {
			results[i] = result
			continue
		}

		result["device_space"] = map[string]interface{}{
			"id":   info.ID,
			"name": info.Name,
		}

		results[i] = result
	}

	next, previous := common.Paginate(totalCount, p, common.BuildBaseURL(c), common.ExtraParams(c))

	return c.JSON(http.StatusOK, common.PaginatedResponse{
		Count:    totalCount,
		Next:     next,
		Previous: previous,
		Results:  results,
	})
}

// GetAutomationSummary returns total, active, and disabled automation counts for a space.
// @Summary Get automation summary
// @Description Returns total, active, and disabled counts for automations in a space. Organization is resolved from X-Organization header or hostname.
// @Tags automations
// @Accept json
// @Produce json
// @Success 200 {object} models.AutomationSummary
// @Failure 400 {object} map[string]string "Invalid request parameters"
// @Failure 500 {object} map[string]string "Internal server error"
// @Router /telemetry/v1/automations/summary [get]
func (h *Handler) GetAutomationsSummary(c echo.Context) error {
	org := common.ResolveOrgFromRequest(c)
	if org == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "Could not determine organization from hostname or X-Organization header",
		})
	}

	spaceSlug, err := common.ResolveSpaceSlugFromRequest(c)
	if err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "X-Space header is required",
		})
	}

	spaceID, err := h.tsClient.GetSpaceIDBySlug(c.Request().Context(), org, spaceSlug)
	if err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": fmt.Sprintf("Space '%s' not found", spaceSlug),
		})
	}

	ctx := timescaledb.ContextWithOrg(c.Request().Context(), org)

	summary, err := h.tsClient.GetAutomationSummary(ctx, spaceID)
	if err != nil {
		h.logger.Error("failed to get automation summary", zap.Error(err))
		return c.JSON(http.StatusInternalServerError, map[string]string{
			"error": "failed to get automation summary",
		})
	}

	return c.JSON(http.StatusOK, summary)
}

// GetAutomationByID returns a single automation by ID
// @Summary Get automation by ID
// @Description Retrieve a single automation by ID. Organization is resolved from X-Organization header or hostname (e.g., {org}.localhost)
// @Tags automations
// @Accept json
// @Produce json
// @Param automation_id path string true "Automation ID"
// @Success 200 {object} map[string]interface{}
// @Failure 400 {object} map[string]string "Invalid request parameters"
// @Failure 404 {object} map[string]string "Automation not found"
// @Failure 500 {object} map[string]string "Internal server error"
// @Router /telemetry/v1/automations/{automation_id} [get]
func (h *Handler) GetAutomationByID(c echo.Context) error {
	org := common.ResolveOrgFromRequest(c)
	if org == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "Could not determine organization from hostname or X-Organization header",
		})
	}

	automationID := strings.TrimSpace(c.Param("automation_id"))
	if automationID == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "automation_id is required",
		})
	}

	ctx := timescaledb.ContextWithOrg(c.Request().Context(), org)

	automation, err := h.tsClient.GetAutomationByID(ctx, automationID)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			return c.JSON(http.StatusNotFound, map[string]string{
				"error": "automation not found",
			})
		}
		h.logger.Error("failed to get automation", zap.String("automation_id", automationID), zap.Error(err))
		return c.JSON(http.StatusInternalServerError, map[string]string{
			"error": "failed to get automation",
		})
	}

	spaceIDStr := ""
	if automation.SpaceID != nil {
		spaceIDStr = automation.SpaceID.String()
	}
	return c.JSON(http.StatusOK, h.convertAutomationToMapWithDeviceSpace(c.Request().Context(), automation, org, spaceIDStr))
}

// CreateAutomation creates a new automation
// @Summary Create automation
// @Description Create a new automation. Organization is resolved from X-Organization header or hostname (e.g., {org}.localhost)
// @Tags automations
// @Accept json
// @Produce json
// @Param request body apimodels.AutomationRequest true "Automation configuration"
// @Success 201 {object} map[string]interface{}
// @Failure 400 {object} map[string]string "Invalid request parameters"
// @Failure 500 {object} map[string]string "Internal server error"
// @Router /telemetry/v1/automations [post]
func (h *Handler) CreateAutomation(c echo.Context) error {
	org := common.ResolveOrgFromRequest(c)
	if org == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "Could not determine organization from hostname or X-Organization header",
		})
	}

	var req apimodels.AutomationRequest
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "invalid request body",
		})
	}

	// Resolve space_id from X-Space header
	spaceSlug, err := common.ResolveSpaceSlugFromRequest(c)
	if err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "X-Space header is required",
		})
	}

	spaceID, err := h.tsClient.GetSpaceIDBySlug(c.Request().Context(), org, spaceSlug)
	if err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": fmt.Sprintf("Space '%s' not found", spaceSlug),
		})
	}
	req.SpaceID = &spaceID

	// Validate required fields
	if req.Name == nil || strings.TrimSpace(*req.Name) == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "name is required",
		})
	}

	if strings.TrimSpace(req.DeviceID) == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "device_id is required",
		})
	}

	if _, err := uuid.Parse(req.DeviceID); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "invalid device_id format",
		})
	}

	if len(req.ActionIDs) == 0 {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "action_ids is required",
		})
	}

	for _, actionID := range req.ActionIDs {
		if _, err := uuid.Parse(actionID); err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": "invalid action_id format",
			})
		}
	}

	// Validate event_rule fields if provided
	if req.EventRule != nil {
		if req.EventRule.RuleKey == nil || strings.TrimSpace(*req.EventRule.RuleKey) == "" {
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": "event_rule.rule_key is required when event_rule is provided",
			})
		}
	}

	ctx := timescaledb.ContextWithOrg(c.Request().Context(), org)

	automation, err := h.tsClient.CreateAutomation(ctx, &req)
	if err != nil {
		h.logger.Error("failed to create automation", zap.Error(err))
		return c.JSON(http.StatusInternalServerError, map[string]string{
			"error": "failed to create automation",
		})
	}

	spaceIDStr := ""
	if automation.SpaceID != nil {
		spaceIDStr = automation.SpaceID.String()
	}
	return c.JSON(http.StatusCreated, h.convertAutomationToMapWithDeviceSpace(c.Request().Context(), automation, org, spaceIDStr))
}

// UpdateAutomation updates an existing automation
// @Summary Update automation
// @Description Update an existing automation by ID. Organization is resolved from X-Organization header or hostname (e.g., {org}.localhost)
// @Tags automations
// @Accept json
// @Produce json
// @Param automation_id path string true "Automation ID"
// @Param request body apimodels.AutomationRequest true "Automation configuration"
// @Success 200 {object} map[string]interface{}
// @Failure 400 {object} map[string]string "Invalid request parameters"
// @Failure 404 {object} map[string]string "Automation not found"
// @Failure 500 {object} map[string]string "Internal server error"
// @Router /telemetry/v1/automations/{automation_id} [put]
func (h *Handler) UpdateAutomation(c echo.Context) error {
	org := common.ResolveOrgFromRequest(c)
	if org == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "Could not determine organization from hostname or X-Organization header",
		})
	}

	automationID := strings.TrimSpace(c.Param("automation_id"))
	if automationID == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "automation_id is required",
		})
	}

	var req apimodels.AutomationRequest
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "invalid request body",
		})
	}

	// Validate required fields
	if req.Name == nil || strings.TrimSpace(*req.Name) == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "name is required",
		})
	}

	if strings.TrimSpace(req.DeviceID) == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "device_id is required",
		})
	}

	if _, err := uuid.Parse(req.DeviceID); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "invalid device_id format",
		})
	}

	if len(req.ActionIDs) == 0 {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "action_ids is required",
		})
	}

	for _, actionID := range req.ActionIDs {
		if _, err := uuid.Parse(actionID); err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": "invalid action_id format",
			})
		}
	}

	// Validate event_rule fields if provided
	if req.EventRule != nil {
		if req.EventRule.RuleKey == nil || strings.TrimSpace(*req.EventRule.RuleKey) == "" {
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": "event_rule.rule_key is required when event_rule is provided",
			})
		}
	}

	ctx := timescaledb.ContextWithOrg(c.Request().Context(), org)

	automation, err := h.tsClient.UpdateAutomation(ctx, automationID, &req)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			return c.JSON(http.StatusNotFound, map[string]string{
				"error": "automation not found",
			})
		}
		h.logger.Error("failed to update automation", zap.String("automation_id", automationID), zap.Error(err))
		return c.JSON(http.StatusInternalServerError, map[string]string{
			"error": "failed to update automation",
		})
	}

	spaceIDStr := ""
	if automation.SpaceID != nil {
		spaceIDStr = automation.SpaceID.String()
	}

	return c.JSON(http.StatusOK, h.convertAutomationToMapWithDeviceSpace(c.Request().Context(), automation, org, spaceIDStr))
}

// DeleteAutomation deletes an automation
// @Summary Delete automation
// @Description Delete an automation by ID. Organization is resolved from X-Organization header or hostname (e.g., {org}.localhost)
// @Tags automations
// @Accept json
// @Produce json
// @Param automation_id path string true "Automation ID"
// @Success 200 {object} map[string]string
// @Failure 400 {object} map[string]string "Invalid request parameters"
// @Failure 404 {object} map[string]string "Automation not found"
// @Failure 500 {object} map[string]string "Internal server error"
// @Router /telemetry/v1/automations/{automation_id} [delete]
func (h *Handler) DeleteAutomation(c echo.Context) error {
	org := common.ResolveOrgFromRequest(c)
	if org == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "Could not determine organization from hostname or X-Organization header",
		})
	}

	automationID := strings.TrimSpace(c.Param("automation_id"))
	if automationID == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "automation_id is required",
		})
	}

	ctx := timescaledb.ContextWithOrg(c.Request().Context(), org)

	_, err := h.tsClient.DeleteAutomation(ctx, automationID)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			return c.JSON(http.StatusNotFound, map[string]string{
				"error": "automation not found",
			})
		}
		h.logger.Error("failed to delete automation", zap.String("automation_id", automationID), zap.Error(err))
		return c.JSON(http.StatusInternalServerError, map[string]string{
			"error": "failed to delete automation",
		})
	}

	return c.JSON(http.StatusOK, map[string]string{
		"message": "automation deleted successfully",
	})
}

// GetActions returns actions with pagination and search
// @Summary Get actions
// @Description Retrieve actions with optional search. Organization is resolved from X-Organization header or hostname (e.g., {org}.localhost)
// @Tags automations
// @Accept json
// @Produce json
// @Param search query string false "Search by name or key"
// @Param limit query int false "Number of results per page (default 20)"
// @Param offset query int false "Number of results to skip (default 0)"
// @Success 200 {object} common.PaginatedResponse
// @Failure 400 {object} map[string]string "Invalid request parameters"
// @Failure 500 {object} map[string]string "Internal server error"
// @Router /telemetry/v1/actions [get]
func (h *Handler) GetActions(c echo.Context) error {
	org := common.ResolveOrgFromRequest(c)
	if org == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "Could not determine organization from hostname or X-Organization header",
		})
	}

	search := strings.TrimSpace(c.QueryParam("search"))

	// Pagination
	p := common.ParsePagination(c)

	ctx := timescaledb.ContextWithOrg(c.Request().Context(), org)

	actions, totalCount, err := h.tsClient.GetActions(ctx, search, p.Limit, p.Offset)
	if err != nil {
		h.logger.Error("failed to get actions", zap.Error(err))
		return c.JSON(http.StatusInternalServerError, map[string]string{
			"error": "failed to get actions",
		})
	}

	// Convert to response format
	results := make([]map[string]interface{}, len(actions))
	for i, a := range actions {
		results[i] = convertActionToMap(&a)
	}

	next, previous := common.Paginate(totalCount, p, common.BuildBaseURL(c), common.ExtraParams(c))

	return c.JSON(http.StatusOK, common.PaginatedResponse{
		Count:    totalCount,
		Next:     next,
		Previous: previous,
		Results:  results,
	})
}

// CreateAction creates a new action
// @Summary Create action
// @Description Create a new action. Organization is resolved from X-Organization header or hostname (e.g., {org}.localhost)
// @Tags automations
// @Accept json
// @Produce json
// @Param request body apimodels.ActionRequest true "Action configuration"
// @Success 201 {object} map[string]interface{}
// @Failure 400 {object} map[string]string "Invalid request parameters"
// @Failure 500 {object} map[string]string "Internal server error"
// @Router /telemetry/v1/actions [post]
func (h *Handler) CreateAction(c echo.Context) error {
	org := common.ResolveOrgFromRequest(c)
	if org == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "Could not determine organization from hostname or X-Organization header",
		})
	}

	var req apimodels.ActionRequest
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "invalid request body",
		})
	}

	// Validate required fields
	if strings.TrimSpace(req.Name) == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "name is required",
		})
	}

	if strings.TrimSpace(req.Key) == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "key is required",
		})
	}

	ctx := timescaledb.ContextWithOrg(c.Request().Context(), org)

	action, err := h.tsClient.CreateAction(ctx, req.Name, req.Key, req.Data)
	if err != nil {
		h.logger.Error("failed to create action", zap.Error(err))
		return c.JSON(http.StatusInternalServerError, map[string]string{
			"error": "failed to create action",
		})
	}

	return c.JSON(http.StatusCreated, convertActionToMap(action))
}

// UpdateAction updates an existing action
// @Summary Update action
// @Description Update an existing action by ID. Organization is resolved from X-Organization header or hostname (e.g., {org}.localhost)
// @Tags automations
// @Accept json
// @Produce json
// @Param action_id path string true "Action ID"
// @Param request body apimodels.ActionRequest true "Action configuration"
// @Success 200 {object} map[string]interface{}
// @Failure 400 {object} map[string]string "Invalid request parameters"
// @Failure 404 {object} map[string]string "Action not found"
// @Failure 500 {object} map[string]string "Internal server error"
// @Router /telemetry/v1/actions/{action_id} [put]
func (h *Handler) UpdateAction(c echo.Context) error {
	org := common.ResolveOrgFromRequest(c)
	if org == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "Could not determine organization from hostname or X-Organization header",
		})
	}

	actionID := strings.TrimSpace(c.Param("action_id"))
	if actionID == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "action_id is required",
		})
	}

	var req apimodels.ActionRequest
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "invalid request body",
		})
	}

	// Validate required fields
	if strings.TrimSpace(req.Name) == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "name is required",
		})
	}

	if strings.TrimSpace(req.Key) == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "key is required",
		})
	}

	ctx := timescaledb.ContextWithOrg(c.Request().Context(), org)

	action, err := h.tsClient.UpdateAction(ctx, actionID, req.Name, req.Key, req.Data)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			return c.JSON(http.StatusNotFound, map[string]string{
				"error": "action not found",
			})
		}
		h.logger.Error("failed to update action", zap.String("action_id", actionID), zap.Error(err))
		return c.JSON(http.StatusInternalServerError, map[string]string{
			"error": "failed to update action",
		})
	}

	return c.JSON(http.StatusOK, convertActionToMap(action))
}

// DeleteAction deletes an action
// @Summary Delete action
// @Description Delete an action by ID. Organization is resolved from X-Organization header or hostname (e.g., {org}.localhost)
// @Tags automations
// @Accept json
// @Produce json
// @Param action_id path string true "Action ID"
// @Success 200 {object} map[string]string
// @Failure 400 {object} map[string]string "Invalid request parameters"
// @Failure 404 {object} map[string]string "Action not found"
// @Failure 500 {object} map[string]string "Internal server error"
// @Router /telemetry/v1/actions/{action_id} [delete]
func (h *Handler) DeleteAction(c echo.Context) error {
	org := common.ResolveOrgFromRequest(c)
	if org == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "Could not determine organization from hostname or X-Organization header",
		})
	}

	actionID := strings.TrimSpace(c.Param("action_id"))
	if actionID == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "action_id is required",
		})
	}

	ctx := timescaledb.ContextWithOrg(c.Request().Context(), org)

	err := h.tsClient.DeleteAction(ctx, actionID)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			return c.JSON(http.StatusNotFound, map[string]string{
				"error": "action not found",
			})
		}
		h.logger.Error("failed to delete action", zap.String("action_id", actionID), zap.Error(err))
		return c.JSON(http.StatusInternalServerError, map[string]string{
			"error": "failed to delete action",
		})
	}

	return c.JSON(http.StatusOK, map[string]string{
		"message": "action deleted successfully",
	})
}

// convertAutomationToMapWithDeviceSpace enriches automation response with device space information
func (h *Handler) convertAutomationToMapWithDeviceSpace(ctx context.Context, a *models.AutomationWithActions, organization, spaceID string) map[string]interface{} {
	result := convertAutomationToMap(a)

	if a.DeviceID == "" || spaceID == "" {
		return result
	}

	deviceSpace, err := h.deviceServiceClient.GetDeviceSpaceByDeviceID(
		ctx,
		a.DeviceID,
		organization,
		spaceID,
	)
	if err != nil {
		h.logger.Warn("failed to fetch device space info",
			zap.String("device_id", a.DeviceID),
			zap.Error(err),
		)
		return result
	}

	if deviceSpace == nil {
		h.logger.Info("no device space found",
			zap.String("device_id", a.DeviceID),
		)
		return result
	}

	result["device_space"] = map[string]interface{}{
		"id":   deviceSpace.ID,
		"name": deviceSpace.Name,
	}

	return result
}

// Helper functions to convert models to maps
func convertAutomationToMap(a *models.AutomationWithActions) map[string]interface{} {
	result := map[string]interface{}{
		"id":         a.ID,
		"name":       a.Name,
		"title":      a.Title,
		"device_id":  a.DeviceID,
		"updated_at": a.UpdatedAt,
		"created_at": a.CreatedAt,
	}

	if a.EventRule != nil {
		eventRule := map[string]interface{}{
			"event_rule_id": a.EventRule.EventRuleID,
			"rule_key":      a.EventRule.RuleKey,
		}
		if a.EventRule.Definition != nil {
			eventRule["definition"] = a.EventRule.Definition
		}
		if a.EventRule.IsActive != nil {
			eventRule["is_active"] = a.EventRule.IsActive
		}
		if a.EventRule.RepeatAble != nil {
			eventRule["repeat_able"] = a.EventRule.RepeatAble
		}
		if a.EventRule.CooldownSec != nil {
			eventRule["cooldown_sec"] = a.EventRule.CooldownSec
		}
		if a.EventRule.Description != nil {
			eventRule["description"] = a.EventRule.Description
		}
		result["event_rule"] = eventRule
	}

	if len(a.Actions) > 0 {
		actions := make([]map[string]interface{}, len(a.Actions))
		for i, action := range a.Actions {
			actions[i] = convertActionToMap(&action)
		}
		result["actions"] = actions
	}

	return result
}

func convertActionToMap(a *models.Action) map[string]interface{} {
	result := map[string]interface{}{
		"id":         a.ID,
		"name":       a.Name,
		"key":        a.Key,
		"created_at": a.CreatedAt,
	}

	if a.Data != nil {
		// Try to parse data as JSON, if fails return as string
		var dataJSON interface{}
		if err := json.Unmarshal([]byte(*a.Data), &dataJSON); err == nil {
			result["data"] = dataJSON
		} else {
			result["data"] = *a.Data
		}
	}

	return result
}
