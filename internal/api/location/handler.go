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

// resolveOrgFromRequest determines which organization slug to use for DB scoping.
func resolveOrgFromRequest(c echo.Context) (string, string) {
	if orgHeader := c.Request().Header.Get("X-Organization"); orgHeader != "" {
		return orgHeader, "header"
	}
	return "", "unknown"
}

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

		// Resolve organization to use for DB search_path (reusable helper)
		orgToUse, orgSource := resolveOrgFromRequest(c)

		// Log which org will be used for DB scoping
		logger.Info("Selecting DB schema for request",
			zap.String("org_used", orgToUse),
			zap.String("org_source", orgSource),
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
			// Populate basic fields
			lr := models.LocationResponse{
				Timestamp: loc.Time,
				Latitude:  loc.Latitude,
				Longitude: loc.Longitude,
				Accuracy:  loc.Accuracy,
				DeviceID:  loc.DeviceID,
			}

			// Try to load shared attributes for this device at (or before) this timestamp.
			attrs, aerr := tsClient.GetLatestAttributesForDeviceAt(ctx, loc.DeviceID, loc.Time)
			if aerr != nil {
				logger.Error("failed to load attributes for location",
					zap.Error(aerr),
					zap.String("device_id", loc.DeviceID),
				)
			} else {
				lr.Attributes = attrs
			}

			locationResponses[i] = lr
		}

		response := models.LocationHistoryResponse{
			DeviceID:  req.DeviceID,
			SpaceSlug: req.SpaceSlug,
			Count:     len(locationResponses),
			Locations: locationResponses,
			QueryParams: models.QueryParamsResponse{
				Start: req.Start,
				End:   req.End,
				Limit: limit,
			},
		}

		return c.JSON(http.StatusOK, response)
	}
}

func getLastLocation(logger *zap.Logger, tsClient *timescaledb.Client) echo.HandlerFunc {
	return func(c echo.Context) error {
		var r models.LastLocationRequest

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

		// Resolve organization to use for DB search_path (reusable helper)
		orgToUse, orgSource := resolveOrgFromRequest(c)

		// Log which org will be used for DB scoping
		logger.Info("Selecting DB schema for request",
			zap.String("org_used", orgToUse),
			zap.String("org_source", orgSource),
			zap.String("space_slug", req.SpaceSlug),
		)

		ctx := timescaledb.ContextWithOrg(c.Request().Context(), orgToUse)

		// Query database for the last location
		location, err := tsClient.GetLastLocation(ctx, req.DeviceID, req.SpaceSlug)
		if err != nil {
			// Check if it's a "not found" error
			if err.Error() == "sql: no rows in result set" {
				return c.JSON(http.StatusNotFound, map[string]string{
					"error": "no location found for device",
				})
			}

			logger.Error("Failed to query last location",
				zap.Error(err),
				zap.String("device_id", req.DeviceID),
				zap.String("space_slug", req.SpaceSlug),
			)
			return c.JSON(http.StatusInternalServerError, map[string]string{
				"error": "failed to retrieve last location",
			})
		}

		// Convert to response format
		response := models.LastLocationResponse{
			DeviceID:  location.DeviceID,
			SpaceSlug: location.SpaceSlug,
			Timestamp: location.Time,
			Latitude:  location.Latitude,
			Longitude: location.Longitude,
			Accuracy:  location.Accuracy,
		}

		// Attach attributes for last location if available
		if attrs, aerr := tsClient.GetLatestAttributesForDeviceAt(ctx, location.DeviceID, location.Time); aerr == nil {
			// update response to include attributes map - extend LastLocationResponse if desired
			// For now we append into a small response wrapper by returning same fields plus attributes
			// We'll include attributes by using a map response instead of strict struct if needed.
			// To keep compatibility, we will return the response with attributes embedded via an inline map here.
			out := map[string]interface{}{
				"device_id":  response.DeviceID,
				"space_slug": response.SpaceSlug,
				"timestamp":  response.Timestamp,
				"latitude":   response.Latitude,
				"longitude":  response.Longitude,
				"accuracy":   response.Accuracy,
				"attributes": attrs,
			}

			return c.JSON(http.StatusOK, out)
		} else {
			logger.Error("failed to load attributes for last location",
				zap.Error(aerr),
				zap.String("device_id", location.DeviceID),
			)
		}

		return c.JSON(http.StatusOK, response)
	}
}

