package location

import (
	"net/http"

	models "github.com/Space-DF/telemetry-service/internal/api/location/models"
	"github.com/Space-DF/telemetry-service/internal/timescaledb"
	"github.com/labstack/echo/v4"
	"go.uber.org/zap"
)

const (
	DefaultLimit = 100
	MaxLimit     = 24 * 3600 / 30 * 7 // one week
)

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

		// Query database
		locations, err := tsClient.GetLocationHistory(c.Request().Context(), req.DeviceID, req.OrganizationSlug, req.Start, req.End, limit)
		if err != nil {
			logger.Error("Failed to query location history",
				zap.Error(err),
				zap.String("device_id", req.DeviceID),
				zap.String("organization_slug", req.OrganizationSlug),
			)
			return c.JSON(http.StatusInternalServerError, map[string]string{
				"error": "failed to retrieve location history",
			})
		}

		// Convert to response format
		locationResponses := make([]models.LocationResponse, len(locations))
		for i, loc := range locations {
			locationResponses[i] = models.LocationResponse{
				Timestamp:        loc.Time,
				Latitude:         loc.Latitude,
				Longitude:        loc.Longitude,
				AccuracyGateways: loc.AccuracyGateways,
				DeviceEUI:        loc.DeviceEui,
			}
		}

		response := models.LocationHistoryResponse{
			DeviceID:         req.DeviceID,
			OrganizationSlug: req.OrganizationSlug,
			Count:            len(locationResponses),
			Locations:        locationResponses,
			QueryParams: models.QueryParamsResponse{
				Start: req.Start,
				End:   req.End,
				Limit: limit,
			},
		}

		return c.JSON(http.StatusOK, response)
	}
}
