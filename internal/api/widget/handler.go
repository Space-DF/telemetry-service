package widget

import (
	"fmt"
	"net/http"

	"github.com/Space-DF/telemetry-service/internal/api/common"
	"github.com/Space-DF/telemetry-service/internal/api/widget/models"
	"github.com/Space-DF/telemetry-service/internal/timescaledb"
	"github.com/labstack/echo/v4"
	"go.uber.org/zap"
)

func getWidgetData(logger *zap.Logger, tsClient *timescaledb.Client) echo.HandlerFunc {
	return func(c echo.Context) error {
		var req models.WidgetDataRequest

		// Get entity_id from URL path
		req.EntityID = c.Param("entity_id")
		if req.EntityID == "" {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": "entity_id is required"})
		}

		// Bind remaining query parameters
		if err := c.Bind(&req); err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid query parameters"})
		}

		if req.DisplayType == "" {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": "display_type is required"})
		}

		orgSlug := common.ResolveOrgFromRequest(c)
		if orgSlug == "" {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": "organization not found"})
		}

		// Validate time-range requirements
		if req.DisplayType == models.DisplayTypeChart || req.DisplayType == models.DisplayTypeHistogram || req.DisplayType == models.DisplayTypeTable {
			if req.StartTime == nil || req.EndTime == nil {
				return c.JSON(http.StatusBadRequest, map[string]string{
					"error": fmt.Sprintf("%s requires start_time and end_time", req.DisplayType),
				})
			}
		}

		ctx := timescaledb.ContextWithOrg(c.Request().Context(), orgSlug)

		switch req.DisplayType {
		case models.DisplayTypeGauge, models.DisplayTypeSlider, models.DisplayTypeValue:
			return gaugeHandler(c, logger, tsClient, ctx, req)
		case models.DisplayTypeSwitch:
			return switchHandler(c, logger, tsClient, ctx, req)
		case models.DisplayTypeChart:
			return chartHandler(c, logger, tsClient, ctx, req)
		case models.DisplayTypeHistogram:
			return histogramHandler(c, logger, tsClient, ctx, req)
		case models.DisplayTypeTable:
			return tableHandler(c, logger, tsClient, ctx, req)
		case models.DisplayTypeMap:
			return mapHandler(c, logger, tsClient, ctx, req)
		default:
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": fmt.Sprintf("unknown display_type: %s", req.DisplayType),
			})
		}
	}
}
