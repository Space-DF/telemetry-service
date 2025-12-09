package entities

import (
	"net/http"
	"strconv"

	"github.com/Space-DF/telemetry-service/internal/timescaledb"
	"github.com/labstack/echo/v4"
	"go.uber.org/zap"
)

// resolveOrgFromRequest copied from other handlers: prefer X-Organization header
func resolveOrgFromRequest(c echo.Context) (string, string) {
	if orgHeader := c.Request().Header.Get("X-Organization"); orgHeader != "" {
		return orgHeader, "header"
	}
	return "", "unknown"
}

func getEntities(logger *zap.Logger, tsClient *timescaledb.Client) echo.HandlerFunc {
	return func(c echo.Context) error {
		// Parse query params
		category := c.QueryParam("category")
		deviceID := c.QueryParam("device_id")
		pageStr := c.QueryParam("page")
		pageSizeStr := c.QueryParam("page_size")
		spaceSlug := c.QueryParam("space_slug")

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

		// Resolve organization for DB scoping
		orgToUse, orgSource := resolveOrgFromRequest(c)
		logger.Info("Selecting DB schema for entities request", zap.String("org_used", orgToUse), zap.String("org_source", orgSource), zap.String("space_slug", spaceSlug))

		ctx := timescaledb.ContextWithOrg(c.Request().Context(), orgToUse)

		// Query DB
		entities, count, err := tsClient.GetEntities(ctx, spaceSlug, category, deviceID, page, pageSize)
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
