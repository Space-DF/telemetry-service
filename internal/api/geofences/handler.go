package geofences

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"strings"

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
// @Description Retrieve a paginated list of geofences with optional filtering by space and active status. Organization is resolved from X-Organization header or hostname (e.g., {org}.localhost)
// @Tags geofences
// @Accept json
// @Produce json
// @Param is_active query bool false "Filter by active status"
// @Param limit query int false "Number of results per page (default 20)"
// @Param offset query int false "Number of results to skip (default 0)"
// @Success 200 {object} common.PaginatedResponse
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

		req := &apimodels.ListGeofencesRequest{}

		spaceSlug, err := common.ResolveSpaceSlugFromRequest(c)
		if err != nil {
			return c.JSON(http.StatusBadRequest, apimodels.ErrorResponse{
				Error: "X-Space header is required",
			})
		}

		spaceID, err := tsClient.GetSpaceIDBySlug(c.Request().Context(), orgToUse, spaceSlug)
		if err != nil {
			return c.JSON(http.StatusBadRequest, apimodels.ErrorResponse{
				Error: fmt.Sprintf("Space '%s' not found", spaceSlug),
			})
		}

		req.SpaceID = &spaceID

		// Parse is_active query parameter
		if isActiveStr := c.QueryParam("is_active"); isActiveStr != "" {
			isActive, err := strconv.ParseBool(isActiveStr)
			if err == nil {
				req.IsActive = &isActive
			}
		}

		// Parse search query parameter
		if search := c.QueryParam("search"); search != "" {
			req.Search = search
		}

		// Parse bbox query parameter
		var bboxEnvelope *[4]float64
		bboxStr := c.QueryParam("bbox")
		parts := strings.Split(bboxStr, ",")
		if bboxStr != "" && len(parts) != 4 {
			return c.JSON(http.StatusBadRequest, apimodels.ErrorResponse{
				Error: "Invalid bbox format. Expected: west,south,east,north",
			})
		}

		if bboxStr != "" {
			var envelope [4]float64
			for index, part := range parts {
				value, err := strconv.ParseFloat(strings.TrimSpace(part), 64)
				if err != nil {
					return c.JSON(http.StatusBadRequest, apimodels.ErrorResponse{
						Error: "Invalid bbox format. Expected: west,south,east,north",
					})
				}
				envelope[index] = value
			}
			bboxEnvelope = &envelope
		}

		ctx := timescaledb.ContextWithOrg(c.Request().Context(), orgToUse)

		p := common.ParsePagination(c)

		// Query geofences with limit/offset at DB level
		geofences, total, err := tsClient.GetGeofences(ctx, req.SpaceID, req.IsActive, req.Search, bboxEnvelope, p.Limit, p.Offset)
		if err != nil {
			logger.Error("failed to get geofences", zap.Error(err))
			return c.JSON(http.StatusInternalServerError, apimodels.ErrorResponse{
				Error: "Failed to get geofences",
			})
		}

		next, previous := common.Paginate(total, p, common.BuildBaseURL(c), common.ExtraParams(c))

		return c.JSON(http.StatusOK, common.PaginatedResponse{
			Count:    total,
			Next:     next,
			Previous: previous,
			Results:  convertGeofencesToResponses(ctx, tsClient, geofences),
		})
	}
}

func testGeofence(logger *zap.Logger, tsClient *timescaledb.Client) echo.HandlerFunc {
	return func(c echo.Context) error {
		orgToUse := common.ResolveOrgFromRequest(c)
		if orgToUse == "" {
			return c.JSON(http.StatusBadRequest, apimodels.ErrorResponse{
				Error: "Could not determine organization from hostname or X-Organization header",
			})
		}

		spaceSlug, err := common.ResolveSpaceSlugFromRequest(c)
		if err != nil {
			return c.JSON(http.StatusBadRequest, apimodels.ErrorResponse{
				Error: "X-Space header is required",
			})
		}

		spaceID, err := tsClient.GetSpaceIDBySlug(c.Request().Context(), orgToUse, spaceSlug)
		if err != nil {
			return c.JSON(http.StatusBadRequest, apimodels.ErrorResponse{
				Error: fmt.Sprintf("Space '%s' not found", spaceSlug),
			})
		}

		var req apimodels.TestGeofenceRequest
		if err := c.Bind(&req); err != nil {
			return c.JSON(http.StatusBadRequest, apimodels.ErrorResponse{
				Error: "Invalid request body",
			})
		}

		if len(req.Features) == 0 {
			return c.JSON(http.StatusBadRequest, apimodels.ErrorResponse{
				Error: "features are required",
			})
		}

		if req.TypeZone == "" {
			return c.JSON(http.StatusBadRequest, apimodels.ErrorResponse{
				Error: "type_zone is required",
			})
		}

		ctx := timescaledb.ContextWithOrg(c.Request().Context(), orgToUse)

		// Test the geofence geometry only - event rules are created separately

		// Convert array of GeoJSON features to MultiPolygon or Polygon
		multiPolygonGeoJSON, err := convertFeaturesToMultiPolygon(req.Features)
		if err != nil {
			return c.JSON(http.StatusBadRequest, apimodels.ErrorResponse{
				Error: "Invalid features format",
			})
		}

		checks, err := tsClient.TestGeofenceWithGeometry(ctx, spaceID.String(), string(multiPolygonGeoJSON))
		if err != nil {
			return c.JSON(http.StatusInternalServerError, apimodels.ErrorResponse{Error: "Failed to test geofence"})
		}
		if len(checks) == 0 {
			return c.JSON(http.StatusOK, apimodels.ResultResponse{Result: "No devices found in space"})
		}

		totalDevices := len(checks)
		for _, check := range checks {
			if req.TypeZone == "safe" && !check.IsInside {
				return c.JSON(http.StatusBadRequest, apimodels.ResultResponse{
					Result: "Device is outside the safe geofence",
				})
			}

			if req.TypeZone == "danger" && check.IsInside {
				return c.JSON(http.StatusBadRequest, apimodels.ResultResponse{
					Result: "Device is inside the danger geofence",
				})
			}
		}

		return c.JSON(http.StatusOK, apimodels.ResultResponse{
			Result: fmt.Sprintf("All %d devices passed geofence test", totalDevices),
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

		return c.JSON(http.StatusOK, convertGeofenceToResponse(ctx, tsClient, geofence))
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

		if len(req.Features) == 0 {
			return c.JSON(http.StatusBadRequest, apimodels.ErrorResponse{
				Error: "features is required and must be a non-empty array of GeoJSON Feature objects",
			})
		}

		if req.Color == "" {
			return c.JSON(http.StatusBadRequest, apimodels.ErrorResponse{
				Error: "color is required",
			})
		}

		multiPolygonGeoJSON, err := convertFeaturesToMultiPolygon(req.Features)
		if err != nil {
			logger.Error("failed to convert geometry to multipolygon", zap.Error(err))
			return c.JSON(http.StatusBadRequest, apimodels.ErrorResponse{
				Error: "Invalid geometry format",
			})
		}

		ctx := timescaledb.ContextWithOrg(c.Request().Context(), orgToUse)
		// Pass definition to CreateGeofence which will create the event rule with it
		var definition *json.RawMessage
		if len(req.Definition) > 0 {
			definition = &req.Definition
		}
		geofence, err := tsClient.CreateGeofence(ctx, req.Name, req.Type, multiPolygonGeoJSON, req.Features, req.Color, req.SpaceID, req.IsActive, nil, definition)
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

		// Build response with event rule information
		response := apimodels.CreateGeofenceResponse{
			Message:  "Geofence created successfully",
			Geofence: convertModelGeofenceToResponse(ctx, tsClient, geofence),
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
		if len(req.Features) > 0 {
			geometryJSON, err = convertFeaturesToMultiPolygon(req.Features)
			if err != nil {
				logger.Error("failed to convert geometry to multipolygon", zap.Error(err))
				return c.JSON(http.StatusBadRequest, apimodels.ErrorResponse{
					Error: "Invalid geometry format",
				})
			}
		}

		ctx := timescaledb.ContextWithOrg(c.Request().Context(), orgToUse)

		// event_rule_id is managed separately - not updated through geofence endpoint
		var eventRuleID *uuid.UUID

		geofence, err := tsClient.UpdateGeofence(ctx, geofenceID, req.Name, req.Type, geometryJSON, req.Features, req.Color, req.SpaceID, req.IsActive, eventRuleID)
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
		if len(req.Definition) > 0 && geofence.EventRuleID != nil {
			eventRule, err := tsClient.GetEventRuleByID(ctx, geofence.EventRuleID.String())
			if err == nil && eventRule != nil {
				defStr := string(req.Definition)
				updateReq := &models.EventRule{
					RuleKey:     eventRule.RuleKey,
					Definition:  &defStr,
					IsActive:    eventRule.IsActive,
					RepeatAble:  eventRule.RepeatAble,
					CooldownSec: eventRule.CooldownSec,
					Description: eventRule.Description,
				}
				err = tsClient.UpdateEventRule(ctx, eventRule.EventRuleID, updateReq)
				if err != nil {
					logger.Warn("failed to update event rule definition",
						zap.String("geofence_id", geofenceIDStr),
						zap.String("event_rule_id", eventRule.EventRuleID),
						zap.Error(err))
				}
			}
		}

		// Build response with event rule information
		respGeofence := convertModelGeofenceToResponse(ctx, tsClient, geofence)

		response := apimodels.UpdateGeofenceResponse{
			Message:  "Geofence updated successfully",
			Geofence: respGeofence,
		}

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

// convertGeofencesToResponses converts geofences to API responses including event rule information
func convertGeofencesToResponses(ctx context.Context, tsClient *timescaledb.Client, geofences []models.GeofenceWithSpace) []apimodels.GeofenceResponse {
	responses := make([]apimodels.GeofenceResponse, len(geofences))
	for i, g := range geofences {
		responses[i] = convertGeofenceToResponse(ctx, tsClient, &g)
	}
	return responses
}

// convertGeofenceToResponse converts a single geofence to API response including event rule information
func convertGeofenceToResponse(ctx context.Context, tsClient *timescaledb.Client, g *models.GeofenceWithSpace) apimodels.GeofenceResponse {
	resp := apimodels.GeofenceResponse{
		GeofenceID: g.GeofenceID,
		Name:       g.Name,
		TypeZone:   g.TypeZone,
		Features:   g.Features,
		Color:      g.Color,
		IsActive:   g.IsActive,
		SpaceID:    g.SpaceID,
		CreatedAt:  g.CreatedAt,
		UpdatedAt:  g.UpdatedAt,
	}

	// Fetch event rule for this geofence
	if g.EventRuleID != nil {
		eventRule, err := tsClient.GetEventRuleByID(ctx, g.EventRuleID.String())
		if err == nil && eventRule != nil {
			ruleKey := eventRule.RuleKey
			isActive := false
			if eventRule.IsActive != nil {
				isActive = *eventRule.IsActive
			}
			resp.EventRule = &apimodels.EventRuleInfo{
				EventRuleID: eventRule.EventRuleID,
				RuleKey:     ruleKey,
				IsActive:    isActive,
				CreatedAt:   eventRule.CreatedAt,
			}
			if eventRule.Definition != nil {
				resp.EventRule.Definition = json.RawMessage(*eventRule.Definition)
			}
		}
	}

	return resp
}

func convertModelGeofenceToResponse(ctx context.Context, tsClient *timescaledb.Client, g *models.Geofence) apimodels.GeofenceResponse {
	resp := apimodels.GeofenceResponse{
		GeofenceID: g.GeofenceID,
		Name:       g.Name,
		TypeZone:   g.TypeZone,
		Features:   g.Features,
		Color:      g.Color,
		IsActive:   g.IsActive,
		SpaceID:    g.SpaceID,
		CreatedAt:  g.CreatedAt,
		UpdatedAt:  g.UpdatedAt,
	}

	// Fetch event rule for this geofence
	if g.EventRuleID != nil {
		eventRule, err := tsClient.GetEventRuleByID(ctx, g.EventRuleID.String())
		if err == nil && eventRule != nil {
			isActive := false
			if eventRule.IsActive != nil {
				isActive = *eventRule.IsActive
			}
			resp.EventRule = &apimodels.EventRuleInfo{
				EventRuleID: eventRule.EventRuleID,
				RuleKey:     eventRule.RuleKey,
				IsActive:    isActive,
				CreatedAt:   eventRule.CreatedAt,
			}
			if eventRule.Definition != nil {
				resp.EventRule.Definition = json.RawMessage(*eventRule.Definition)
			}
		}
	}

	return resp
}

// Converts an array of GeoJSON Polygon objects to a MultiPolygon GeoJSON
func convertFeaturesToMultiPolygon(features []json.RawMessage) ([]byte, error) {
	if len(features) == 0 {
		return nil, fmt.Errorf("no features provided")
	}

	var allCoordinates [][][][]float64

	for _, feature := range features {
		var geom struct {
			Type        string        `json:"type"`
			Coordinates [][][]float64 `json:"coordinates"`
		}
		if err := json.Unmarshal(feature, &geom); err != nil {
			return nil, fmt.Errorf("failed to unmarshal feature geometry: %w", err)
		}

		if geom.Type != "Polygon" {
			return nil, fmt.Errorf("expected Polygon geometry, got %s", geom.Type)
		}

		allCoordinates = append(allCoordinates, geom.Coordinates)
	}

	multiPolygon := map[string]interface{}{
		"type":        "MultiPolygon",
		"coordinates": allCoordinates,
	}

	return json.Marshal(multiPolygon)
}
