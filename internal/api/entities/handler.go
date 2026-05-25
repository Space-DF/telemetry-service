package entities

import (
	"net/http"
	"strings"

	"github.com/Space-DF/telemetry-service/internal/api/common"
	"github.com/Space-DF/telemetry-service/internal/api/entities/models"
	"github.com/Space-DF/telemetry-service/internal/timescaledb"
	"github.com/labstack/echo/v4"
	"go.uber.org/zap"
)

// GetEntities godoc
// @Summary Get entities
// @Description Retrieve a paginated list of entities with optional filtering. Organization is resolved from X-Organization header or hostname (e.g., {org}.localhost)
// @Tags entities
// @Accept json
// @Produce json
// @Param category query string false "Filter by entity category"
// @Param device_id query string false "Filter by device ID"
// @Param dev_eui query string false "Filter by device EUI"
// @Param display_type query string false "Filter by display type (comma-separated)"
// @Param search query string false "Search term for filtering"
// @Param limit query int false "Number of results per page (default 20)"
// @Param offset query int false "Number of results to skip (default 0)"
// @Success 200 {object} common.PaginatedResponse
// @Failure 400 {object} map[string]string "Invalid request parameters"
// @Failure 500 {object} map[string]string "Internal server error"
// @Router /telemetry/v1/entities [get]
func getEntities(logger *zap.Logger, tsClient *timescaledb.Client) echo.HandlerFunc {
	return func(c echo.Context) error {
		// Parse query params
		req := &models.EntitiesRequest{
			Category:     c.QueryParam("category"),
			DeviceID:     c.QueryParam("device_id"),
			DevEUI:       strings.TrimSpace(c.QueryParam("dev_eui")),
			DisplayTypes: parseDisplayTypes(c.QueryParam("display_type")),
			Search:       strings.TrimSpace(c.QueryParam("search")),
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

		p := common.ParsePagination(c)

		entities, total, err := tsClient.GetEntities(ctx, req.Category, req.DeviceID, req.DevEUI, req.DisplayTypes, req.Search, p.Limit, p.Offset)
		if err != nil {
			logger.Error("failed to query entities", zap.Error(err))
			return c.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to query entities"})
		}

		next, previous := common.Paginate(total, p, common.BuildBaseURL(c), common.ExtraParams(c))

		return c.JSON(http.StatusOK, common.PaginatedResponse{
			Count:    total,
			Next:     next,
			Previous: previous,
			Results:  entities,
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

// UpdateEntities godoc
// @Summary Bulk update entities
// @Description Bulk update entities in the current space. When `all=true`, the request applies `is_enabled` to every entity in the space and `excluded_entity_ids` receive the opposite value. When `all=false`, only the provided `visible_entity_ids` and `hidden_entity_ids` are updated. Organization is resolved from X-Organization header or hostname and space is resolved from X-Space.
// @Tags entities
// @Accept json
// @Produce json
// @Param request body models.UpdateEntityRequest true "Entity updates"
// @Success 200 {object} timescaledb.BulkUpdateEntitiesResult
// @Failure 400 {object} map[string]string "Invalid request"
// @Failure 500 {object} map[string]string "Internal server error"
// @Router /telemetry/v1/entities/bulk-update [put]
func updateEntities(logger *zap.Logger, tsClient *timescaledb.Client) echo.HandlerFunc {
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

		var req models.UpdateEntityRequest
		if err := c.Bind(&req); err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": "Invalid request body",
			})
		}

		all := req.All != nil && *req.All

		ctx := timescaledb.ContextWithOrg(c.Request().Context(), orgToUse)
		if all {
			if req.IsEnabled == nil {
				return c.JSON(http.StatusBadRequest, map[string]string{
					"error": "is_enabled must be provided when all is true",
				})
			}
			isEnabled := *req.IsEnabled
			if len(req.VisibleEntityIDs) > 0 || len(req.HiddenEntityIDs) > 0 {
				return c.JSON(http.StatusBadRequest, map[string]string{
					"error": "When all is true, use excluded_entity_ids instead of visible_entity_ids or hidden_entity_ids",
				})
			}
			result, err := tsClient.UpdateEntitiesBySelection(ctx, req.ExcludedEntityIDs, spaceSlug, isEnabled)
			if err != nil {
				return c.JSON(http.StatusInternalServerError, map[string]string{
					"error": "Failed to update entities",
				})
			}

			return c.JSON(http.StatusOK, result)
		}

		if len(req.VisibleEntityIDs) == 0 && len(req.HiddenEntityIDs) == 0 {
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": "visible_entity_ids or hidden_entity_ids must be provided",
			})
		}
		if len(req.ExcludedEntityIDs) > 0 {
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": "excluded_entity_ids is only supported when all is true",
			})
		}
		entityUpdates := make([]timescaledb.EntityEnabledUpdate, 0, len(req.VisibleEntityIDs)+len(req.HiddenEntityIDs))
		for _, entityID := range req.VisibleEntityIDs {
			entityUpdates = append(entityUpdates, timescaledb.EntityEnabledUpdate{
				EntityID:  entityID,
				IsEnabled: true,
			})
		}
		for _, entityID := range req.HiddenEntityIDs {
			entityUpdates = append(entityUpdates, timescaledb.EntityEnabledUpdate{
				EntityID:  entityID,
				IsEnabled: false,
			})
		}
		result, err := tsClient.UpdateEntities(ctx, entityUpdates, spaceSlug)
		if err != nil {
			return c.JSON(http.StatusInternalServerError, map[string]string{
				"error": "Failed to update entities",
			})
		}

		return c.JSON(http.StatusOK, result)
	}
}
