package entities

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/Space-DF/telemetry-service/internal/api/common"
	"github.com/Space-DF/telemetry-service/internal/timescaledb"
	"github.com/labstack/echo/v4"
	"go.uber.org/zap"
)

// updateDeviceTriggerEventRequest represents the request to update an device's trigger event type
type updateDeviceTriggerEventRequest struct {
	TriggerEventType string `json:"trigger_event_type"`
}

func getEntities(logger *zap.Logger, tsClient *timescaledb.Client) echo.HandlerFunc {
	return func(c echo.Context) error {
		// Parse query params
		category := c.QueryParam("category")
		deviceID := c.QueryParam("device_id")
		displayTypes := parseDisplayTypes(c.QueryParam("display_type"))
		search := strings.TrimSpace(c.QueryParam("search"))
		pageStr := c.QueryParam("page")
		pageSizeStr := c.QueryParam("page_size")

		// Resolve space slug from X-Space header (required)
		spaceSlug, err := common.ResolveSpaceSlugFromRequest(c)
		if err != nil {
			return err
		}

		// defaults
		page := 1
		pageSize := 100
		if pageStr != "" {
			if p, err := strconv.Atoi(pageStr); err == nil && p > 0 {
				page = p
			}
		}
		if pageSizeStr != "" {
			if ps, err := strconv.Atoi(pageSizeStr); err == nil && ps > 0 {
				pageSize = ps
			}
		}

		// Resolve organization from hostname or X-Organization header
		orgToUse := common.ResolveOrgFromRequest(c)
		if orgToUse == "" {
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": "Could not determine organization from hostname or X-Organization header",
			})
		}

		logger.Info("Selecting DB schema for entities request",
			zap.String("org_used", orgToUse))

		ctx := timescaledb.ContextWithOrg(c.Request().Context(), orgToUse)

		// Query DB
		entities, count, err := tsClient.GetEntities(ctx, spaceSlug, category, deviceID, displayTypes, search, page, pageSize)
		if err != nil {
			logger.Error("failed to query entities", zap.Error(err))
			return c.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to query entities"})
		}

		return c.JSON(http.StatusOK, map[string]interface{}{
			"count":   count,
			"results": entities,
		})
	}
}

func parseDisplayTypes(param string) []string {
	if param == "" {
		return nil
	}

	parts := strings.Split(param, ",")
	j := 0
	for i := range parts {
		if trimmed := strings.TrimSpace(parts[i]); trimmed != "" {
			parts[j] = trimmed
			j++
		}
	}

	if j == 0 {
		return nil
	}
	return parts[:j]
}

// updateDeviceTriggerEvent updates the trigger event type for an device
func updateDeviceTriggerEvent(logger *zap.Logger, tsClient *timescaledb.Client) echo.HandlerFunc {
	return func(c echo.Context) error {
		deviceID := strings.TrimSpace(c.Param("device_id"))
		if deviceID == "" {
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": "device_id is required",
			})
		}

		var req updateDeviceTriggerEventRequest
		if err := c.Bind(&req); err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": "invalid request body",
			})
		}

		if req.TriggerEventType == "" {
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": "trigger_event_type is required",
			})
		}

		orgToUse := common.ResolveOrgFromRequest(c)
		if orgToUse == "" {
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": "Could not determine organization from hostname or X-Organization header",
			})
		}

		ctx := timescaledb.ContextWithOrg(c.Request().Context(), orgToUse)
		err := tsClient.UpdateDeviceTriggerEventType(ctx, deviceID, req.TriggerEventType)
		if err != nil {
			logger.Error("failed to update device trigger event type",
				zap.String("device_id", deviceID),
				zap.String("trigger_event_type", req.TriggerEventType),
				zap.Error(err))
			return c.JSON(http.StatusInternalServerError, map[string]string{
				"error": "failed to update device trigger event type",
			})
		}

		return c.JSON(http.StatusOK, map[string]interface{}{
			"device_id":           deviceID,
			"trigger_event_type":  req.TriggerEventType,
		})
	}
}
