package alerts

import (
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

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

type Alert struct {
	ID         string                 `json:"id"`
	Type       string                 `json:"type"`
	Level      string                 `json:"level"`
	Message    string                 `json:"message"`
	EntityID   string                 `json:"entity_id"`
	EntityName string                 `json:"entity_name"`
	DeviceID   string                 `json:"device_id"`
	SpaceSlug  string                 `json:"space_slug"`
	Location   *LocationInfo          `json:"location,omitempty"`
	WaterDepth float64                `json:"water_depth"`
	Unit       string                 `json:"unit"`
	Threshold  *ThresholdInfo         `json:"threshold"`
	ReportedAt time.Time              `json:"reported_at"`
	Attributes map[string]interface{} `json:"attributes,omitempty"`
}

type LocationInfo struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	Address   string  `json:"address,omitempty"`
}

type ThresholdInfo struct {
	Warning  float64 `json:"warning"`
	Critical float64 `json:"critical"`
}

type AlertsResponse struct {
	Results    []interface{} `json:"results"`
	TotalCount int           `json:"total_count"`
	Page       int           `json:"page"`
	PageSize   int           `json:"page_size"`
}

// GetAlerts returns alerts based on water level thresholds
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
	dateFilter := strings.TrimSpace(c.QueryParam("date"))

	if deviceID == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "device_id is required",
		})
	}
	// Pagination
	page, _ := strconv.Atoi(c.QueryParam("page"))
	if page < 1 {
		page = 1
	}
	pageSize, _ := strconv.Atoi(c.QueryParam("page_size"))
	if pageSize < 1 || pageSize > 100 {
		pageSize = 20
	}

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
		dateFilter,
		cautionThreshold,
		warningThreshold,
		criticalThreshold,
		page,
		pageSize,
	)

	if err != nil {
		switch {
		case errors.Is(err, timescaledb.ErrDateRequired):
			return c.JSON(http.StatusBadRequest, map[string]string{"error": "date is required (YYYY-MM-DD)"})
		case errors.Is(err, timescaledb.ErrInvalidDateFormat):
			return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid date format, expected YYYY-MM-DD"})
		}
		h.logger.Error("Failed to get alerts", zap.Error(err))
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to retrieve alerts"})
	}

	response := AlertsResponse{
		Results:    alerts,
		TotalCount: totalCount,
		Page:       page,
		PageSize:   pageSize,
	}

	return c.JSON(http.StatusOK, response)
}
