package data

import (
	"net/http"
	"time"

	"github.com/Space-DF/telemetry-service/internal/api/common"
	models "github.com/Space-DF/telemetry-service/internal/api/data/models"
	"github.com/Space-DF/telemetry-service/internal/timescaledb"
	"github.com/labstack/echo/v4"
	"go.uber.org/zap"
)

// GetDeviceEntityPropertiesBatch godoc
// @Summary Get device entity properties in batch
// @Description Retrieve entities with latest values for multiple devices in one response.
// @Tags data
// @Accept json
// @Produce json
// @Param request body models.GetDevicePropertiesBatchRequest true "Device IDs"
// @Success 200 {object} map[string][]map[string]interface{}
// @Failure 400 {object} map[string]string "Invalid request body"
// @Failure 500 {object} map[string]string "Internal server error"
// @Router /telemetry/v1/data/entity-properties/batch [post]
func getDeviceEntityPropertiesBatch(logger *zap.Logger, tsClient *timescaledb.Client) echo.HandlerFunc {
	return func(c echo.Context) error {
		var r models.GetDevicePropertiesBatchRequest

		if err := c.Bind(&r); err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": "invalid request body",
			})
		}

		orgToUse := common.ResolveOrgFromRequest(c)
		ctx := timescaledb.ContextWithOrg(c.Request().Context(), orgToUse)

		result := make(map[string][]map[string]interface{}, len(r.DeviceIDs))
		seen := make(map[string]struct{}, len(r.DeviceIDs))
		for _, deviceID := range r.DeviceIDs {
			if deviceID == "" {
				continue
			}
			if _, ok := seen[deviceID]; ok {
				continue
			}
			seen[deviceID] = struct{}{}

			var endDate *time.Time
			if r.EndDates != nil {
				if rawEndDate := r.EndDates[deviceID]; rawEndDate != "" {
					parsed, parseErr := time.Parse(time.RFC3339Nano, rawEndDate)
					if parseErr == nil {
						parsed = parsed.UTC()
						endDate = &parsed
					}
				}
			}

			entities, err := tsClient.GetDeviceEntityProperties(ctx, deviceID, endDate)
			if err != nil {
				logger.Error("Failed to query device entity properties",
					zap.Error(err),
					zap.String("device_id", deviceID),
				)
				return c.JSON(http.StatusInternalServerError, map[string]string{
					"error": "failed to retrieve device entity properties",
				})
			}
			result[deviceID] = entities
		}

		return c.JSON(http.StatusOK, result)
	}
}
