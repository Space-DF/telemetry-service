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
// @Param display_type query string false "Filter by display type (comma-separated)"
// @Param search query string false "Search term for filtering"
// @Param limit query int false "Number of results per page (default 20)"
// @Param offset query int false "Number of results to skip (default 0)"
// @Success 200 {object} common.PaginatedResponse
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

		entities, total, err := tsClient.GetEntities(ctx, req.SpaceSlug, req.Category, req.DeviceID, req.DisplayTypes, req.Search, p.Limit, p.Offset)
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
