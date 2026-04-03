package location

import (
	"net/http"

	"github.com/Space-DF/telemetry-service/internal/api/common"
	models "github.com/Space-DF/telemetry-service/internal/api/location/models"
	"github.com/Space-DF/telemetry-service/internal/timescaledb"
	"github.com/labstack/echo/v4"
	"go.uber.org/zap"
)

const (
	DefaultLimit = 100
	MaxLimit     = 24 * 3600 / 30 * 7 // one week
)

// GetLocationHistory godoc
// @Summary Get device location history
// @Description Retrieve paginated historical location data for a specific device within a space. Organization is resolved from X-Organization header or hostname (e.g., {org}.localhost)
// @Tags location
// @Accept json
// @Produce json
// @Param device_id query string true "Device ID"
// @Param space_slug query string true "Space slug"
// @Param start query string true "Start time (RFC3339 format)"
// @Param end query string false "End time (RFC3339 format, defaults to now)"
// @Param limit query int false "Number of results per page (default 100)"
// @Param offset query int false "Number of results to skip (default 0)"
// @Success 200 {object} common.PaginatedResponse
// @Failure 400 {object} map[string]string "Invalid request parameters"
// @Failure 500 {object} map[string]string "Internal server error"
// @Router /telemetry/v1/location/history [get]
func getLocationHistory(logger *zap.Logger, tsClient *timescaledb.Client) echo.HandlerFunc {
	return func(c echo.Context) error {
		var r models.LocationHistoryRequest

		// Bind query parameters
		if err := c.Bind(&r); err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": "invalid query parameters",
			})
		}
		req, err := r.Validate()
		if err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": err.Error(),
			})
		}

		// Resolve organization from hostname or X-Organization header
		orgToUse := common.ResolveOrgFromRequest(c)

		// Log which org will be used for DB scoping
		logger.Info("Selecting DB schema for request",
			zap.String("org_used", orgToUse),
			zap.String("space_slug", req.SpaceSlug),
		)

		// Build context with org for DB search_path
		ctx := timescaledb.ContextWithOrg(c.Request().Context(), orgToUse)

		// Fetch all matching locations (DB uses a hard limit of MaxLimit)
		allLocations, err := tsClient.GetLocationHistory(ctx, req.DeviceID, req.SpaceSlug, req.Start, req.End, MaxLimit)
		if err != nil {
			logger.Error("Failed to query location history",
				zap.Error(err),
				zap.String("device_id", req.DeviceID),
				zap.String("space_slug", req.SpaceSlug),
			)
			return c.JSON(http.StatusInternalServerError, map[string]string{
				"error": "failed to retrieve location history",
			})
		}

		// Convert to response format
		allLocationResponses := make([]models.LocationResponse, len(allLocations))
		for i, loc := range allLocations {
			allLocationResponses[i] = models.LocationResponse{
				Timestamp: loc.Time,
				Latitude:  loc.Latitude,
				Longitude: loc.Longitude,
				DeviceID:  loc.DeviceID,
			}
		}

		total := len(allLocationResponses)
		p := common.ParsePagination(c)
		start, end := common.SlicePage(total, p)

		next, previous := common.Paginate(total, p, common.BuildBaseURL(c), common.ExtraParams(c))

		return c.JSON(http.StatusOK, common.PaginatedResponse{
			Count:    total,
			Next:     next,
			Previous: previous,
			Results:  allLocationResponses[start:end],
		})
	}
}
