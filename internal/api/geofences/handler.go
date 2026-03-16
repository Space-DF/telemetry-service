package geofences

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"strings"

	"github.com/Space-DF/telemetry-service/internal/api/common"
	apimodels "github.com/Space-DF/telemetry-service/internal/api/geofences/models"
	"github.com/Space-DF/telemetry-service/internal/events/evaluator"
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
			Results:  convertGeofencesToResponsesWithEventRules(ctx, tsClient, geofences),
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

		var condEvaluator *evaluator.ConditionEvaluator

		if len(req.Definition) == 0 {
			return c.JSON(http.StatusBadRequest, apimodels.ErrorResponse{
				Error: "definition is required",
			})
		}

		definitionStr := string(req.Definition)

		var defData map[string]interface{}
		err = json.Unmarshal(req.Definition, &defData)

		hasConditions := false
		if err == nil {
			_, hasConditions = defData["conditions"]
		}

		if err == nil && !hasConditions {
			wrapped := map[string]interface{}{
				"conditions": defData,
			}

			if b, err := json.Marshal(wrapped); err == nil {
				definitionStr = string(b)
			}
		}

		if definitionStr != "" {
			condEvaluator = evaluator.NewConditionEvaluator(logger)
		}

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

			if condEvaluator != nil {
				evalCtx := map[string]interface{}{
					"latitude":                  check.Latitude,
					"longitude":                 check.Longitude,
					"reported_at":               time.Now(),
					"distance_from_geofence_km": check.DistanceKm,
				}
				matched, _, _ := condEvaluator.EvaluateDefinitionWithContext(definitionStr, evalCtx)
				if matched {
					continue
				}
			}

			return c.JSON(http.StatusBadRequest, apimodels.ResultResponse{
				Result: fmt.Sprintf("Device %s failed geofence test", check.DeviceID),
			})
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
		geofence, err := tsClient.CreateGeofence(ctx, req.Name, req.Type, multiPolygonGeoJSON, req.Features, req.Color, req.SpaceID, req.IsActive)
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
				Features:   geofence.Features,
				Color:      geofence.Color,
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
		geofence, err := tsClient.UpdateGeofence(ctx, geofenceID, req.Name, req.Type, geometryJSON, req.Features, req.Color, req.SpaceID, req.IsActive)
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
		respGeofence := convertModelGeofenceToResponse(geofence)
		response := apimodels.UpdateGeofenceResponse{
			Message:  "Geofence updated successfully",
			Geofence: respGeofence,
		}

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
			DeviceID: deviceID,
			Results:  convertGeofencesToResponsesWithEventRules(ctx, tsClient, geofences),
			Count:    len(geofences),
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
		Features:   g.Features,
		Color:      g.Color,
		IsActive:   g.IsActive,
		SpaceID:    g.SpaceID,
		CreatedAt:  g.CreatedAt,
		UpdatedAt:  g.UpdatedAt,
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
		Features:   g.Features,
		Color:      g.Color,
		IsActive:   g.IsActive,
		SpaceID:    g.SpaceID,
		CreatedAt:  g.CreatedAt,
		UpdatedAt:  g.UpdatedAt,
	}
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
