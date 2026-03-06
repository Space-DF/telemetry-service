package geofences

import (
	"context"
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
// @Summary Get geofences
// @Description Retrieve all geofences with optional filtering by space and active status. Organization is resolved from X-Organization header or hostname (e.g., {org}.localhost)
// @Tags geofences
// @Accept json
// @Produce json
// @Param space_id query string false "Filter by Space UUID"
// @Param is_active query bool false "Filter by active status"
// @Param page query int false "Page number (default 1)"
// @Param page_size query int false "Page size (default 20, max 100)"
// @Success 200 {object} models.GeofencesListResponse
// @Failure 400 {object} models.ErrorResponse
// @Failure 500 {object} models.ErrorResponse
// @Router /telemetry/v1/geofences [get]
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
			Geofences:  convertGeofencesToResponsesWithEventRules(ctx, tsClient, geofences),
			TotalCount: total,
			Page:       req.Page,
			PageSize:   req.PageSize,
		})
	}
}

// getGeofenceByID returns a single geofence by ID
// @Summary Get geofence by ID
// @Description Retrieve a single geofence by its UUID. Organization is resolved from X-Organization header or hostname (e.g., {org}.localhost)
// @Tags geofences
// @Accept json
// @Produce json
// @Param geofence_id path string true "Geofence UUID"
// @Success 200 {object} models.GeofenceResponse
// @Failure 400 {object} models.ErrorResponse
// @Failure 404 {object} models.ErrorResponse
// @Failure 500 {object} models.ErrorResponse
// @Router /telemetry/v1/geofences/{geofence_id} [get]
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

		return c.JSON(http.StatusOK, convertGeofenceToResponseWithEventRule(ctx, tsClient, geofence))
	}
}

// createGeofence creates a new geofence
// @Summary Create geofence
// @Description Create a new geofence with polygon geometry for a space. Organization is resolved from X-Organization header or hostname (e.g., {org}.localhost)
// @Tags geofences
// @Accept json
// @Produce json
// @Param request body models.CreateGeofenceRequest true "Geofence configuration"
// @Success 201 {object} models.CreateGeofenceResponse
// @Failure 400 {object} models.ErrorResponse
// @Failure 500 {object} models.ErrorResponse
// @Router /telemetry/v1/geofences [post]
func createGeofence(logger *zap.Logger, tsClient *timescaledb.Client) echo.HandlerFunc {
	return func(c echo.Context) error {
		orgToUse := common.ResolveOrgFromRequest(c)
		if orgToUse == "" {
			return c.JSON(http.StatusBadRequest, apimodels.ErrorResponse{
				Error: "Could not determine organization from hostname or X-Organization header",
			})
		}

		// Resolve space_id from X-Space header
		spaceSlug, err := common.ResolveSpaceSlugFromRequest(c)
		if err != nil {
			return c.JSON(http.StatusBadRequest, apimodels.ErrorResponse{
				Error: "X-Space header is required",
			})
		}

		spaceID, err := tsClient.GetSpaceIDBySlug(c.Request().Context(), orgToUse, spaceSlug)
		if err != nil {
			logger.Error("failed to resolve space from X-Space header",
				zap.String("space_slug", spaceSlug),
				zap.String("org", orgToUse),
				zap.Error(err))
			return c.JSON(http.StatusBadRequest, apimodels.ErrorResponse{
				Error: fmt.Sprintf("Space '%s' not found", spaceSlug),
			})
		}

		var req apimodels.CreateGeofenceRequest
		if err := c.Bind(&req); err != nil {
			return c.JSON(http.StatusBadRequest, apimodels.ErrorResponse{
				Error: "Invalid request body",
			})
		}

		// Override space_id with the one resolved from header
		req.SpaceID = &spaceID

		// Convert geometry array to MultiPolygon geometry
		multiPolygonGeoJSON, err := convertFeaturesToMultiPolygon(req.Geometry)
		if err != nil {
			logger.Error("failed to convert geometry to multipolygon", zap.Error(err))
			return c.JSON(http.StatusBadRequest, apimodels.ErrorResponse{
				Error: "Invalid geometry format",
			})
		}

		ctx := timescaledb.ContextWithOrg(c.Request().Context(), orgToUse)
		geofence, err := tsClient.CreateGeofence(ctx, req.Name, req.Type, multiPolygonGeoJSON, req.SpaceID, req.IsActive)
		if err != nil {
			logger.Error("failed to create geofence",
				zap.String("name", req.Name),
				zap.String("type", req.Type),
				zap.String("error_detail", err.Error()),
				zap.Error(err))

			// Return more specific error message for debugging
			errorMsg := "Failed to create geofence"
			if err.Error() != "" {
				errorMsg = fmt.Sprintf("Failed to create geofence: %s", err.Error())
			}

			return c.JSON(http.StatusInternalServerError, apimodels.ErrorResponse{
				Error: errorMsg,
			})
		}

		// Create event rule with rule_key="geofence" for the geofence
		ruleKey := "geofence"
		isActive := true
		geofenceIDStr := geofence.GeofenceID.String()
		description := geofence.Name

		eventRuleReq := &models.EventRuleRequest{
			RuleKey:     &ruleKey,
			GeofenceID:  &geofenceIDStr,
			IsActive:    &isActive,
			Description: &description,
		}

		// Add definition from request if provided
		if len(req.Definition) > 0 {
			defStr := string(req.Definition)
			eventRuleReq.Definition = &defStr
		}

		// Add SpaceID if provided
		if geofence.SpaceID != nil {
			spaceIDStr := geofence.SpaceID.String()
			eventRuleReq.SpaceID = &spaceIDStr
		}

		_, err = tsClient.CreateEventRule(ctx, eventRuleReq)
		if err != nil {
			logger.Warn("failed to create event rule for geofence",
				zap.String("geofence_id", geofence.GeofenceID.String()),
				zap.Error(err))
		} else {
			logger.Info("successfully created event rule for geofence",
				zap.String("geofence_id", geofence.GeofenceID.String()),
				zap.String("rule_key", ruleKey))
		}

		// Fetch the created event rule for the response
		var eventRuleInfo *apimodels.EventRuleInfo
		eventRules, err := tsClient.GetEventRulesByGeofenceID(ctx, geofence.GeofenceID.String())
		if err == nil && len(eventRules) > 0 {
			eventRule := eventRules[0]
			if eventRule.RuleKey != nil && eventRule.IsActive != nil {
				eventRuleInfo = &apimodels.EventRuleInfo{
					EventRuleID: eventRule.EventRuleID,
					RuleKey:     *eventRule.RuleKey,
					IsActive:    *eventRule.IsActive,
					CreatedAt:   eventRule.CreatedAt,
				}
				if eventRule.Definition != nil {
					eventRuleInfo.Definition = json.RawMessage(*eventRule.Definition)
				}
			} else {
				logger.Warn("event rule has nil RuleKey or IsActive; skipping EventRuleInfo in response",
					zap.String("geofence_id", geofence.GeofenceID.String()),
				)
			}
		}

		// Build response with event rule information
		response := apimodels.CreateGeofenceResponse{
			Message: "Geofence created successfully",
			Geofence: apimodels.GeofenceResponse{
				GeofenceID: geofence.GeofenceID,
				Name:       geofence.Name,
				TypeZone:   geofence.TypeZone,
				Geometry:   json.RawMessage(geofence.Geometry),
				EventRule:  eventRuleInfo,
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
// @Summary Update geofence
// @Description Update an existing geofence by ID. Organization is resolved from X-Organization header or hostname (e.g., {org}.localhost)
// @Tags geofences
// @Accept json
// @Produce json
// @Param geofence_id path string true "Geofence UUID"
// @Param request body models.UpdateGeofenceRequest true "Geofence updates"
// @Success 200 {object} models.UpdateGeofenceResponse
// @Failure 400 {object} models.ErrorResponse
// @Failure 404 {object} models.ErrorResponse
// @Failure 500 {object} models.ErrorResponse
// @Router /telemetry/v1/geofences/{geofence_id} [put]
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

		// Convert geometry array to MultiPolygon geometry if provided
		var geometryJSON []byte
		if len(req.Geometry) > 0 {
			geometryJSON, err = convertFeaturesToMultiPolygon(req.Geometry)
			if err != nil {
				logger.Error("failed to convert geometry to multipolygon", zap.Error(err))
				return c.JSON(http.StatusBadRequest, apimodels.ErrorResponse{
					Error: "Invalid geometry format",
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
			logger.Error("failed to update geofence",
				zap.String("geofence_id", geofenceIDStr),
				zap.String("error_detail", err.Error()),
				zap.Error(err))

			// Return a stable, generic error message to avoid exposing internal details
			return c.JSON(http.StatusInternalServerError, apimodels.ErrorResponse{
				Error: "Failed to update geofence",
			})
		}

		// Update event rule definition if provided
		if len(req.Definition) > 0 {
			eventRules, err := tsClient.GetEventRulesByGeofenceID(ctx, geofenceID.String())
			if err == nil && len(eventRules) > 0 {
				eventRule := eventRules[0]
				defStr := string(req.Definition)
				updateReq := &models.EventRuleRequest{
					RuleKey:    eventRule.RuleKey,
					GeofenceID: eventRule.GeofenceID,
					Definition: &defStr,
					IsActive:   eventRule.IsActive,
				}

				_, err = tsClient.UpdateEventRule(ctx, eventRule.EventRuleID, updateReq)
				if err != nil {
					logger.Warn("failed to update event rule definition",
						zap.String("geofence_id", geofenceIDStr),
						zap.String("event_rule_id", eventRule.EventRuleID),
						zap.Error(err))
				}
			}
		}

		// Build response with event rule information
		response := apimodels.UpdateGeofenceResponse{
			Message: "Geofence updated successfully",
		}
		respGeofence := convertModelGeofenceToResponse(geofence)

		// Fetch event rules for this geofence
		eventRules, err := tsClient.GetEventRulesByGeofenceID(ctx, geofence.GeofenceID.String())
		if err == nil && len(eventRules) > 0 {
			eventRule := eventRules[0]
			// Safely handle potentially nil pointer fields
			var ruleKey string
			if eventRule.RuleKey != nil {
				ruleKey = *eventRule.RuleKey
			}
			isActive := false
			if eventRule.IsActive != nil {
				isActive = *eventRule.IsActive
			}
			respGeofence.EventRule = &apimodels.EventRuleInfo{
				EventRuleID: eventRule.EventRuleID,
				RuleKey:     ruleKey,
				IsActive:    isActive,
				CreatedAt:   eventRule.CreatedAt,
			}
			if eventRule.Definition != nil {
				respGeofence.EventRule.Definition = json.RawMessage(*eventRule.Definition)
			}
		}

		response.Geofence = respGeofence

		return c.JSON(http.StatusOK, response)
	}
}

// deleteGeofence deletes a geofence
// @Summary Delete geofence
// @Description Delete a geofence by ID. Organization is resolved from X-Organization header or hostname (e.g., {org}.localhost)
// @Tags geofences
// @Accept json
// @Produce json
// @Param geofence_id path string true "Geofence UUID"
// @Success 200 {object} models.DeleteGeofenceResponse
// @Failure 400 {object} models.ErrorResponse
// @Failure 404 {object} models.ErrorResponse
// @Failure 500 {object} models.ErrorResponse
// @Router /telemetry/v1/geofences/{geofence_id} [delete]
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

		eventRules, err := tsClient.GetEventRulesByGeofenceID(ctx, geofenceID.String())
		if err == nil {
			for _, eventRule := range eventRules {
				if deleteErr := tsClient.DeleteEventRule(ctx, eventRule.EventRuleID); deleteErr != nil {
					logger.Warn("failed to delete associated event rule",
						zap.String("geofence_id", geofenceIDStr),
						zap.String("event_rule_id", eventRule.EventRuleID),
						zap.Error(deleteErr))
				}
			}
		}

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
// @Summary Get geofences by device
// @Description Retrieve all geofences associated with a specific device. Organization is resolved from X-Organization header or hostname (e.g., {org}.localhost)
// @Tags geofences
// @Accept json
// @Produce json
// @Param device_id path string true "Device ID"
// @Success 200 {object} models.GeofencesByDeviceResponse
// @Failure 400 {object} models.ErrorResponse
// @Failure 500 {object} models.ErrorResponse
// @Router /telemetry/v1/geofences/device/{device_id} [get]
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
			Geofences: convertGeofencesToResponsesWithEventRules(ctx, tsClient, geofences),
			Count:     len(geofences),
		})
	}
}

// convertGeofencesToResponsesWithEventRules converts geofences to responses including event rule information
func convertGeofencesToResponsesWithEventRules(ctx context.Context, tsClient *timescaledb.Client, geofences []models.GeofenceWithSpace) []apimodels.GeofenceResponse {
	responses := make([]apimodels.GeofenceResponse, len(geofences))
	for i, g := range geofences {
		responses[i] = convertGeofenceToResponseWithEventRule(ctx, tsClient, &g)
	}
	return responses
}

// convertGeofenceToResponseWithEventRule converts a single geofence to response including event rule information
func convertGeofenceToResponseWithEventRule(ctx context.Context, tsClient *timescaledb.Client, g *models.GeofenceWithSpace) apimodels.GeofenceResponse {
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

	// Fetch event rules for this geofence
	eventRules, err := tsClient.GetEventRulesByGeofenceID(ctx, g.GeofenceID.String())
	if err == nil && len(eventRules) > 0 {
		eventRule := eventRules[0]
		resp.EventRule = &apimodels.EventRuleInfo{
			EventRuleID: eventRule.EventRuleID,
			RuleKey:     *eventRule.RuleKey,
			IsActive:    *eventRule.IsActive,
			CreatedAt:   eventRule.CreatedAt,
		}
		if eventRule.Definition != nil {
			resp.EventRule.Definition = json.RawMessage(*eventRule.Definition)
		}
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
			Type        string        `json:"type"`
			Coordinates [][][]float64 `json:"coordinates"`
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
