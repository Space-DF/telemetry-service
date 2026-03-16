package alerts

import (
	"errors"
	"net/http"
	"strconv"
	"strings"

	alertregistry "github.com/Space-DF/telemetry-service/internal/alerts/registry"
	"github.com/Space-DF/telemetry-service/internal/api/common"
	"github.com/Space-DF/telemetry-service/internal/timescaledb"
	"github.com/labstack/echo/v4"
	"go.uber.org/zap"
)

type Handler struct {
	logger   *zap.Logger
	tsClient *timescaledb.Client
}

func NewHandler(logger *zap.Logger, tsClient *timescaledb.Client) *Handler {
	return &Handler{
		logger:   logger,
		tsClient: tsClient,
	}
}

// GetAlerts returns alerts based on water level thresholds
// @Summary Get alerts
// @Description Retrieve alerts based on configurable thresholds for a specific device. Organization is resolved from X-Organization header or hostname (e.g., {org}.localhost)
// @Tags alerts
// @Accept json
// @Produce json
// @Param device_id query string true "Device ID"
// @Param category query string true "Alert category (e.g., water_level)"
// @Param start_date query string true "Start date (YYYY-MM-DD format)"
// @Param end_date query string true "End date (YYYY-MM-DD format)"
// @Param caution_threshold query number false "Caution threshold value"
// @Param warning_threshold query number false "Warning threshold value"
// @Param critical_threshold query number false "Critical threshold value"
// @Param limit query int false "Number of results per page (default 20)"
// @Param offset query int false "Number of results to skip (default 0)"
// @Success 200 {object} common.PaginatedResponse
// @Failure 400 {object} map[string]string "Invalid request parameters"
// @Failure 500 {object} map[string]string "Internal server error"
// @Router /telemetry/v1/alerts [get]
func (h *Handler) GetAlerts(c echo.Context) error {
	// Resolve organization from hostname or X-Organization header
	orgSlug := common.ResolveOrgFromRequest(c)
	if orgSlug == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "Could not determine organization from hostname or X-Organization header",
		})
	}

	h.logger.Info("Getting alerts", zap.String("org", orgSlug))

	// Parse query parameters
	spaceSlug, spaceErr := common.ResolveSpaceSlugFromRequest(c)
	if spaceErr != nil {
		return spaceErr
	}
	deviceID := c.QueryParam("device_id")
	category := c.QueryParam("category")

	processor, ok := alertregistry.Get(category)
	if !ok {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "unsupported category",
		})
	}
	startDate := strings.TrimSpace(c.QueryParam("start_date"))
	endDate := strings.TrimSpace(c.QueryParam("end_date"))

	if deviceID == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "device_id is required",
		})
	}
	// Pagination
	p := common.ParsePagination(c)

	cautionThreshold := processor.DefaultCautionThreshold()
	warningThreshold := processor.DefaultWarningThreshold()
	criticalThreshold := processor.DefaultCriticalThreshold()

	if ct := c.QueryParam("caution_threshold"); ct != "" {
		if val, err := strconv.ParseFloat(ct, 64); err == nil {
			cautionThreshold = val
		}
	}
	if wt := c.QueryParam("warning_threshold"); wt != "" {
		if val, err := strconv.ParseFloat(wt, 64); err == nil {
			warningThreshold = val
		}
	}
	if crt := c.QueryParam("critical_threshold"); crt != "" {
		if val, err := strconv.ParseFloat(crt, 64); err == nil {
			criticalThreshold = val
		}
	}

	alerts, totalCount, err := h.tsClient.GetAlerts(
		c.Request().Context(),
		orgSlug,
		category,
		spaceSlug,
		deviceID,
		startDate,
		endDate,
		cautionThreshold,
		warningThreshold,
		criticalThreshold,
		p.Limit,
		p.Offset,
	)

	if err != nil {
		switch {
		case errors.Is(err, timescaledb.ErrDateRequired):
			return c.JSON(http.StatusBadRequest, map[string]string{"error": "start_date and end_date are required"})
		case errors.Is(err, timescaledb.ErrInvalidDateFormat):
			return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid date format, expected YYYY-MM-DD"})
		}
		h.logger.Error("Failed to get alerts", zap.Error(err))
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to retrieve alerts"})
	}

	next, previous := common.Paginate(totalCount, p, common.BuildBaseURL(c), common.ExtraParams(c))

	return c.JSON(http.StatusOK, common.PaginatedResponse{
		Count:    totalCount,
		Next:     next,
		Previous: previous,
		Results:  alerts,
	})
}
