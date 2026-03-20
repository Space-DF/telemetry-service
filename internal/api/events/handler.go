package events

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/Space-DF/telemetry-service/internal/api/common"
	apimodels "github.com/Space-DF/telemetry-service/internal/api/events/models"
	"github.com/Space-DF/telemetry-service/internal/models"
	"github.com/Space-DF/telemetry-service/internal/timescaledb"
	"github.com/labstack/echo/v4"
	"go.uber.org/zap"
)

// getEventsByDevice returns all events for a specific device
// @Summary Get events by device
// @Description Retrieve events for a specific device with pagination and optional time range filtering. Organization is resolved from X-Organization header or hostname (e.g., {org}.localhost)
// @Tags events
// @Accept json
// @Produce json
// @Param device_id path string true "Device ID"
// @Param limit query int false "Number of results per page (default 20)"
// @Param offset query int false "Number of results to skip (default 0)"
// @Param start_time query int64 false "Start time as Unix timestamp (milliseconds)"
// @Param end_time query int64 false "End time as Unix timestamp (milliseconds)"
// @Param title query string false "Filter by title (case-insensitive partial match)"
// @Success 200 {object} common.PaginatedResponse
// @Failure 400 {object} map[string]string "Invalid request parameters"
// @Failure 500 {object} map[string]string "Internal server error"
// @Router /telemetry/v1/events/device/{device_id} [get]
func getEventsByDevice(logger *zap.Logger, tsClient *timescaledb.Client) echo.HandlerFunc {
	return func(c echo.Context) error {
		orgToUse := common.ResolveOrgFromRequest(c)
		if orgToUse == "" {
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": "Could not determine organization from hostname or X-Organization header",
			})
		}

		deviceID := strings.TrimSpace(c.Param("device_id"))
		if deviceID == "" {
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": "device_id is required",
			})
		}

		// Parse pagination
		p := common.ParsePagination(c)

		// Parse optional time range filters
		var startTime, endTime *int64
		if startTimeStr := c.QueryParam("start_time"); startTimeStr != "" {
			if ms, err := strconv.ParseInt(startTimeStr, 10, 64); err == nil {
				startTime = &ms
			}
		}
		if endTimeStr := c.QueryParam("end_time"); endTimeStr != "" {
			if ms, err := strconv.ParseInt(endTimeStr, 10, 64); err == nil {
				endTime = &ms
			}
		}

		// Parse title search
		var titleSearch *string
		if titleStr := c.QueryParam("search"); titleStr != "" {
			titleSearch = &titleStr
		}

		ctx := timescaledb.ContextWithOrg(c.Request().Context(), orgToUse)
		events, totalCount, err := tsClient.GetEventsByDevice(ctx, orgToUse, deviceID, p.Limit, p.Offset, startTime, endTime, titleSearch)
		if err != nil {
			logger.Error("failed to get events by device",
				zap.String("device_id", deviceID),
				zap.Error(err))
			return c.JSON(http.StatusInternalServerError, map[string]string{
				"error": "failed to get events",
			})
		}

		results := convertEventsToItems(events)
		next, previous := common.Paginate(totalCount, p, common.BuildBaseURL(c), common.ExtraParams(c))

		return c.JSON(http.StatusOK, common.PaginatedResponse{
			Count:    totalCount,
			Next:     next,
			Previous: previous,
			Results:  results,
		})
	}
}

// convertEventsToItems converts internal models.Event to API response items
func convertEventsToItems(events []models.Event) []apimodels.EventItem {
	items := make([]apimodels.EventItem, len(events))
	for i, e := range events {
		item := apimodels.EventItem{
			EventID:    e.EventID,
			EventType:  e.EventType,
			EventLevel: safeString(e.EventLevel),
			Title:      e.Title,
			EntityID:   safeString(e.EntityID),
			TimeFired:  e.TimeFired(),
		}

		// Build automation summary if present
		if e.AutomationID != nil {
			item.Automation = &apimodels.AutomationSummary{
				ID:   *e.AutomationID,
				Name: e.AutomationName,
			}
		}

		// Build geofence summary if present
		if e.GeofenceID != nil {
			item.Geofence = &apimodels.GeofenceSummary{
				ID:       *e.GeofenceID,
				Name:     e.GeofenceName,
				TypeZone: e.GeofenceTypeZone,
			}
		}

		// Add location if available
		if e.Location != nil {
			item.Location = &apimodels.LocationData{
				Latitude:  e.Location.Latitude,
				Longitude: e.Location.Longitude,
			}
		}

		items[i] = item
	}
	return items
}

func safeString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
