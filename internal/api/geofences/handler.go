package geofences

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/Space-DF/telemetry-service/internal/api/common"
	apimodels "github.com/Space-DF/telemetry-service/internal/api/geofences/models"
	"github.com/Space-DF/telemetry-service/internal/models"
	"github.com/Space-DF/telemetry-service/internal/timescaledb"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"go.uber.org/zap"
)

// getGeofences returns all geofences with optional filters
func getGeofences(logger *zap.Logger, tsClient *timescaledb.Client) echo.HandlerFunc {
	return func(c echo.Context) error {
		orgToUse := common.ResolveOrgFromRequest(c)
		if orgToUse == "" {
			return c.JSON(http.StatusBadRequest, apimodels.ErrorResponse{
				Error: "Could not determine organization from hostname or X-Organization header",
			})
		}

		req := &apimodels.ListGeofencesRequest{
			Page:     1,
			PageSize: 20,
		}

		// Parse space_id query parameter
		if spaceIDStr := c.QueryParam("space_id"); spaceIDStr != "" {
			spaceID, err := uuid.Parse(spaceIDStr)
			if err != nil {
				return c.JSON(http.StatusBadRequest, apimodels.ErrorResponse{
					Error: "Invalid space_id format",
				})
			}
			req.SpaceID = &spaceID
		}

		// Parse is_active query parameter
		if isActiveStr := c.QueryParam("is_active"); isActiveStr != "" {
			isActive, err := strconv.ParseBool(isActiveStr)
			if err == nil {
				req.IsActive = &isActive
			}
		}

		// Parse pagination
		if pageStr := c.QueryParam("page"); pageStr != "" {
			if p, err := strconv.Atoi(pageStr); err == nil && p > 0 {
				req.Page = p
			}
		}
		if pageSizeStr := c.QueryParam("page_size"); pageSizeStr != "" {
			if s, err := strconv.Atoi(pageSizeStr); err == nil && s > 0 && s <= 100 {
				req.PageSize = s
			}
		}

		ctx := timescaledb.ContextWithOrg(c.Request().Context(), orgToUse)
		geofences, total, err := tsClient.GetGeofences(ctx, req.SpaceID, req.IsActive, req.Page, req.PageSize)
		if err != nil {
			logger.Error("failed to get geofences", zap.Error(err))
			return c.JSON(http.StatusInternalServerError, apimodels.ErrorResponse{
				Error: "Failed to get geofences",
			})
		}

		return c.JSON(http.StatusOK, apimodels.GeofencesListResponse{
			Geofences:  convertGeofencesToResponses(geofences),
			TotalCount: total,
			Page:       req.Page,
			PageSize:   req.PageSize,
		})
	}
}

// getGeofenceByID returns a single geofence by ID
func getGeofenceByID(logger *zap.Logger, tsClient *timescaledb.Client) echo.HandlerFunc {
	return func(c echo.Context) error {
		orgToUse := common.ResolveOrgFromRequest(c)
		if orgToUse == "" {
			return c.JSON(http.StatusBadRequest, apimodels.ErrorResponse{
				Error: "Could not determine organization from hostname or X-Organization header",
			})
		}

		geofenceIDStr := c.Param("geofence_id")
		geofenceID, err := uuid.Parse(geofenceIDStr)
		if err != nil {
			return c.JSON(http.StatusBadRequest, apimodels.ErrorResponse{
				Error: "Invalid geofence_id format",
			})
		}

		ctx := timescaledb.ContextWithOrg(c.Request().Context(), orgToUse)
		geofence, err := tsClient.GetGeofenceByID(ctx, geofenceID)
		if err != nil {
			if errors.Is(err, models.ErrGeofenceNotFound) {
				return c.JSON(http.StatusNotFound, apimodels.ErrorResponse{
					Error: "Geofence not found",
				})
			}
			logger.Error("failed to get geofence", zap.String("geofence_id", geofenceIDStr), zap.Error(err))
			return c.JSON(http.StatusInternalServerError, apimodels.ErrorResponse{
				Error: "Failed to get geofence",
			})
		}

		return c.JSON(http.StatusOK, convertGeofenceToResponse(geofence))
	}
}

// createGeofence creates a new geofence
func createGeofence(logger *zap.Logger, tsClient *timescaledb.Client) echo.HandlerFunc {
	return func(c echo.Context) error {
		orgToUse := common.ResolveOrgFromRequest(c)
		if orgToUse == "" {
			return c.JSON(http.StatusBadRequest, apimodels.ErrorResponse{
				Error: "Could not determine organization from hostname or X-Organization header",
			})
		}

		var req apimodels.CreateGeofenceRequest
		if err := c.Bind(&req); err != nil {
			return c.JSON(http.StatusBadRequest, apimodels.ErrorResponse{
				Error: "Invalid request body",
			})
		}

		// Convert features array to MultiPolygon geometry
		multiPolygonGeoJSON, err := convertFeaturesToMultiPolygon(req.Features)
		if err != nil {
			logger.Error("failed to convert features to multipolygon", zap.Error(err))
			return c.JSON(http.StatusBadRequest, apimodels.ErrorResponse{
				Error: "Invalid features geometry format",
			})
		}

		ctx := timescaledb.ContextWithOrg(c.Request().Context(), orgToUse)
		geofence, err := tsClient.CreateGeofence(ctx, req.Name, req.Type, multiPolygonGeoJSON, req.SpaceID, req.IsActive)
		if err != nil {
			logger.Error("failed to create geofence", zap.Error(err))
			return c.JSON(http.StatusInternalServerError, apimodels.ErrorResponse{
				Error: "Failed to create geofence",
			})
		}

		// Build response with original features
		response := apimodels.CreateGeofenceResponse{
			Message: "Geofence created successfully",
			Geofence: apimodels.GeofenceResponse{
				GeofenceID: geofence.GeofenceID,
				Name:       geofence.Name,
				TypeZone:   geofence.TypeZone,
				Geometry:   json.RawMessage(geofence.Geometry),
				Features:   req.Features,
				IsActive:   geofence.IsActive,
				SpaceID:    geofence.SpaceID,
				CreatedAt:  geofence.CreatedAt,
				UpdatedAt:  geofence.UpdatedAt,
			},
		}

		return c.JSON(http.StatusCreated, response)
	}
}

// updateGeofence updates an existing geofence
func updateGeofence(logger *zap.Logger, tsClient *timescaledb.Client) echo.HandlerFunc {
	return func(c echo.Context) error {
		orgToUse := common.ResolveOrgFromRequest(c)
		if orgToUse == "" {
			return c.JSON(http.StatusBadRequest, apimodels.ErrorResponse{
				Error: "Could not determine organization from hostname or X-Organization header",
			})
		}

		geofenceIDStr := c.Param("geofence_id")
		geofenceID, err := uuid.Parse(geofenceIDStr)
		if err != nil {
			return c.JSON(http.StatusBadRequest, apimodels.ErrorResponse{
				Error: "Invalid geofence_id format",
			})
		}

		var req apimodels.UpdateGeofenceRequest
		if err := c.Bind(&req); err != nil {
			return c.JSON(http.StatusBadRequest, apimodels.ErrorResponse{
				Error: "Invalid request body",
			})
		}

		// Convert features array to MultiPolygon geometry if provided
		var geometryJSON []byte
		if len(req.Features) > 0 {
			geometryJSON, err = convertFeaturesToMultiPolygon(req.Features)
			if err != nil {
				logger.Error("failed to convert features to multipolygon", zap.Error(err))
				return c.JSON(http.StatusBadRequest, apimodels.ErrorResponse{
					Error: "Invalid features geometry format",
				})
			}
		}

		ctx := timescaledb.ContextWithOrg(c.Request().Context(), orgToUse)
		geofence, err := tsClient.UpdateGeofence(ctx, geofenceID, req.Name, req.Type, geometryJSON, req.SpaceID, req.IsActive)
		if err != nil {
			if errors.Is(err, models.ErrGeofenceNotFound) {
				return c.JSON(http.StatusNotFound, apimodels.ErrorResponse{
					Error: "Geofence not found",
				})
			}
			logger.Error("failed to update geofence", zap.String("geofence_id", geofenceIDStr), zap.Error(err))
			return c.JSON(http.StatusInternalServerError, apimodels.ErrorResponse{
				Error: "Failed to update geofence",
			})
		}

		// Build response with features if provided
		response := apimodels.UpdateGeofenceResponse{
			Message: "Geofence updated successfully",
		}
		respGeofence := convertModelGeofenceToResponse(geofence)
		if req.Features != nil {
			respGeofence.Features = req.Features
		}
		response.Geofence = respGeofence

		return c.JSON(http.StatusOK, response)
	}
}

// deleteGeofence deletes a geofence
func deleteGeofence(logger *zap.Logger, tsClient *timescaledb.Client) echo.HandlerFunc {
	return func(c echo.Context) error {
		orgToUse := common.ResolveOrgFromRequest(c)
		if orgToUse == "" {
			return c.JSON(http.StatusBadRequest, apimodels.ErrorResponse{
				Error: "Could not determine organization from hostname or X-Organization header",
			})
		}

		geofenceIDStr := c.Param("geofence_id")
		geofenceID, err := uuid.Parse(geofenceIDStr)
		if err != nil {
			return c.JSON(http.StatusBadRequest, apimodels.ErrorResponse{
				Error: "Invalid geofence_id format",
			})
		}

		ctx := timescaledb.ContextWithOrg(c.Request().Context(), orgToUse)
		if err := tsClient.DeleteGeofence(ctx, geofenceID); err != nil {
			if errors.Is(err, models.ErrGeofenceNotFound) {
				return c.JSON(http.StatusNotFound, apimodels.ErrorResponse{
					Error: "Geofence not found",
				})
			}
			logger.Error("failed to delete geofence", zap.String("geofence_id", geofenceIDStr), zap.Error(err))
			return c.JSON(http.StatusInternalServerError, apimodels.ErrorResponse{
				Error: "Failed to delete geofence",
			})
		}

		return c.JSON(http.StatusOK, apimodels.DeleteGeofenceResponse{
			Message: "Geofence deleted successfully",
		})
	}
}

// getGeofencesByDevice returns geofences associated with a device
func getGeofencesByDevice(logger *zap.Logger, tsClient *timescaledb.Client) echo.HandlerFunc {
	return func(c echo.Context) error {
		orgToUse := common.ResolveOrgFromRequest(c)
		if orgToUse == "" {
			return c.JSON(http.StatusBadRequest, apimodels.ErrorResponse{
				Error: "Could not determine organization from hostname or X-Organization header",
			})
		}

		deviceID := c.Param("device_id")
		if deviceID == "" {
			return c.JSON(http.StatusBadRequest, apimodels.ErrorResponse{
				Error: "device_id is required",
			})
		}

		ctx := timescaledb.ContextWithOrg(c.Request().Context(), orgToUse)
		geofences, err := tsClient.GetGeofencesByDevice(ctx, deviceID)
		if err != nil {
			logger.Error("failed to get geofences by device", zap.String("device_id", deviceID), zap.Error(err))
			return c.JSON(http.StatusInternalServerError, apimodels.ErrorResponse{
				Error: "Failed to get geofences",
			})
		}

		return c.JSON(http.StatusOK, apimodels.GeofencesByDeviceResponse{
			DeviceID:  deviceID,
			Geofences: convertGeofencesToResponses(geofences),
			Count:     len(geofences),
		})
	}
}

// Helper functions to convert models
func convertGeofencesToResponses(geofences []models.GeofenceWithSpace) []apimodels.GeofenceResponse {
	responses := make([]apimodels.GeofenceResponse, len(geofences))
	for i, g := range geofences {
		responses[i] = convertGeofenceToResponse(&g)
	}
	return responses
}

func convertGeofenceToResponse(g *models.GeofenceWithSpace) apimodels.GeofenceResponse {
	resp := apimodels.GeofenceResponse{
		GeofenceID: g.GeofenceID,
		Name:       g.Name,
		TypeZone:   g.TypeZone,
		Geometry:   json.RawMessage(g.Geometry),
		IsActive:   g.IsActive,
		SpaceID:    g.SpaceID,
		CreatedAt:  g.CreatedAt,
		UpdatedAt:  g.UpdatedAt,
		SpaceName:  g.SpaceName,
		SpaceSlug:  g.SpaceSlug,
		SpaceLogo:  g.SpaceLogo,
	}
	return resp
}

func convertModelGeofenceToResponse(g *models.Geofence) apimodels.GeofenceResponse {
	return apimodels.GeofenceResponse{
		GeofenceID: g.GeofenceID,
		Name:       g.Name,
		TypeZone:   g.TypeZone,
		Geometry:   json.RawMessage(g.Geometry),
		IsActive:   g.IsActive,
		SpaceID:    g.SpaceID,
		CreatedAt:  g.CreatedAt,
		UpdatedAt:  g.UpdatedAt,
	}
}

// convertFeaturesToMultiPolygon converts an array of feature geometries to a MultiPolygon GeoJSON
func convertFeaturesToMultiPolygon(features []apimodels.GeofenceFeature) ([]byte, error) {
	if len(features) == 0 {
		return nil, fmt.Errorf("no features provided")
	}

	// Extract coordinates from each feature's geometry
	var allCoordinates [][][][]float64

	for _, feature := range features {
		var geom struct {
			Type        string          `json:"type"`
			Coordinates [][][]float64  `json:"coordinates"`
		}

		if err := json.Unmarshal(feature.Geometry, &geom); err != nil {
			return nil, fmt.Errorf("failed to unmarshal feature geometry: %w", err)
		}

		if geom.Type != "Polygon" {
			return nil, fmt.Errorf("expected Polygon geometry, got %s", geom.Type)
		}

		allCoordinates = append(allCoordinates, geom.Coordinates)
	}

	// Create MultiPolygon GeoJSON
	multiPolygon := map[string]interface{}{
		"type":        "MultiPolygon",
		"coordinates": allCoordinates,
	}

	return json.Marshal(multiPolygon)
}