package data

import (
	"net/http"

	"github.com/Space-DF/telemetry-service/internal/api/common"
	models "github.com/Space-DF/telemetry-service/internal/api/data/models"
	"github.com/Space-DF/telemetry-service/internal/timescaledb"
	"github.com/labstack/echo/v4"
	"go.uber.org/zap"
)

func getDeviceProperties(logger *zap.Logger, tsClient *timescaledb.Client) echo.HandlerFunc {
	return func(c echo.Context) error {
		var r models.GetDevicePropertiesRequest

		// Bind query parameters
		if err := c.Bind(&r); err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": "invalid query parameters",
			})
		}

		if r.DeviceID == "" || r.SpaceSlug == "" {
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": "device_id and space_slug are required",
			})
		}

		// Resolve organization from hostname or X-Organization header
		orgToUse := common.ResolveOrgFromRequest(c)

		// Log which org will be used for DB scoping
		logger.Info("Fetching device properties",
			zap.String("org_used", orgToUse),
			zap.String("space_slug", r.SpaceSlug),
			zap.String("device_id", r.DeviceID),
		)

		// Build context with org for DB search_path
		ctx := timescaledb.ContextWithOrg(c.Request().Context(), orgToUse)

		// Query all device properties
		props, err := tsClient.GetDeviceProperties(ctx, r.DeviceID, r.SpaceSlug)
		if err != nil {
			logger.Error("Failed to query device properties",
				zap.Error(err),
				zap.String("device_id", r.DeviceID),
				zap.String("space_slug", r.SpaceSlug),
			)
			return c.JSON(http.StatusInternalServerError, map[string]string{
				"error": "failed to retrieve device properties",
			})
		}

		// Return empty dict if no properties found
		if props == nil {
			props = make(map[string]interface{})
		}

		return c.JSON(http.StatusOK, props)
	}
}
