package entities

import (
	"net/http"
	"strconv"
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
// @Param display_type query string false "Filter by display type (comma-separated)"
// @Param search query string false "Search term for filtering"
// @Param page query int false "Page number (default 1)"
// @Param page_size query int false "Page size (default 20)"
// @Success 200 {object} models.EntitiesResponse
// @Failure 400 {object} models.ErrorResponse "Invalid request parameters"
// @Failure 500 {object} models.ErrorResponse "Internal server error"
// @Router /telemetry/v1/entities [get]
func getEntities(logger *zap.Logger, tsClient *timescaledb.Client) echo.HandlerFunc {
	return func(c echo.Context) error {
		// Parse query params
		req := &models.EntitiesRequest{
			Category:     c.QueryParam("category"),
			DeviceID:     c.QueryParam("device_id"),
			DisplayTypes: parseDisplayTypes(c.QueryParam("display_type")),
			Search:       strings.TrimSpace(c.QueryParam("search")),
		}

		// Resolve space slug from X-Space header (required)
		spaceSlug, err := common.ResolveSpaceSlugFromRequest(c)
		if err != nil {
			return err
		}
		req.SpaceSlug = spaceSlug

		if pageStr := c.QueryParam("page"); pageStr != "" {
			if p, err := strconv.Atoi(pageStr); err == nil && p > 0 {
				req.Page = p
			}
		}
		if pageSizeStr := c.QueryParam("page_size"); pageSizeStr != "" {
			if ps, err := strconv.Atoi(pageSizeStr); err == nil && ps > 0 {
				req.PageSize = ps
			}
		}
		req.SetDefaults()

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
		entities, count, err := tsClient.GetEntities(ctx, req.SpaceSlug, req.Category, req.DeviceID, req.DisplayTypes, req.Search, req.Page, req.PageSize)
		if err != nil {
			logger.Error("failed to query entities", zap.Error(err))
			return c.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to query entities"})
		}

		return c.JSON(http.StatusOK, models.EntitiesResponse{
			Count:   count,
			Results: entities,
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
