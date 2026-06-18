package data

import (
	"net/http"

	"github.com/Space-DF/telemetry-service/internal/api/common"
	models "github.com/Space-DF/telemetry-service/internal/api/data/models"
	"github.com/Space-DF/telemetry-service/internal/timescaledb"
	"github.com/labstack/echo/v4"
	"go.uber.org/zap"
)

// GetDeviceEntityProperties godoc
// @Summary Get device entity properties
// @Description Retrieve device entities with each entity's latest value in a single response. Organization is resolved from X-Organization header or hostname (e.g., {org}.localhost)
// @Tags data
// @Accept json
// @Produce json
// @Param device_id query string true "Device ID"
// @Success 200 {array} map[string]interface{}
// @Failure 400 {object} map[string]string "Invalid request parameters"
// @Failure 500 {object} map[string]string "Internal server error"
// @Router /telemetry/v1/data/entity-properties [get]
func getDeviceEntityProperties(logger *zap.Logger, tsClient *timescaledb.Client) echo.HandlerFunc {
	return func(c echo.Context) error {
		var r models.GetDevicePropertiesRequest

		if err := c.Bind(&r); err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": "invalid query parameters",
			})
		}

		if r.DeviceID == "" {
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": "device_id is required",
			})
		}

		orgToUse := common.ResolveOrgFromRequest(c)
		ctx := timescaledb.ContextWithOrg(c.Request().Context(), orgToUse)

		logger.Info("Fetching device entity properties",
			zap.String("org_used", orgToUse),
			zap.String("device_id", r.DeviceID),
		)

		result, err := tsClient.GetDeviceEntityProperties(ctx, r.DeviceID)
		if err != nil {
			logger.Error("Failed to query device entity properties",
				zap.Error(err),
				zap.String("device_id", r.DeviceID),
			)
			return c.JSON(http.StatusInternalServerError, map[string]string{
				"error": "failed to retrieve device entity properties",
			})
		}

		return c.JSON(http.StatusOK, result)
	}
}
