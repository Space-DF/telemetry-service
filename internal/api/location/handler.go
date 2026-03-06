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
// @Description Retrieve historical location data for a specific device within a space. Organization is resolved from X-Organization header or hostname (e.g., {org}.localhost)
// @Tags location
// @Accept json
// @Produce json
// @Param device_id query string true "Device ID"
// @Param space_slug query string true "Space slug"
// @Param start query string true "Start time (RFC3339 format)"
// @Param end query string false "End time (RFC3339 format, defaults to now)"
// @Param limit query int false "Maximum number of records to return (default 100, max 40320)"
// @Success 200 {object} models.LocationHistoryResponse
// @Failure 400 {object} models.ErrorResponse "Invalid request parameters"
// @Failure 500 {object} models.ErrorResponse "Internal server error"
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

		// Handle limit
		limit := req.Limit
		if limit == 0 {
			limit = DefaultLimit
		} else if limit < 0 {
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": "limit must be positive",
			})
		} else if limit > MaxLimit {
			limit = MaxLimit
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

		// Query database
		locations, err := tsClient.GetLocationHistory(ctx, req.DeviceID, req.SpaceSlug, req.Start, req.End, limit)
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
		locationResponses := make([]models.LocationResponse, len(locations))
		for i, loc := range locations {
			locationResponses[i] = models.LocationResponse{
				Timestamp: loc.Time,
				Latitude:  loc.Latitude,
				Longitude: loc.Longitude,
				DeviceID:  loc.DeviceID,
			}
		}

		response := models.LocationHistoryResponse{
			Count:     len(locationResponses),
			Locations: locationResponses,
		}

		return c.JSON(http.StatusOK, response)
	}
}
