package activity_logs

import (
	"net/http"

	"github.com/Space-DF/telemetry-service/internal/api/common"
	"github.com/Space-DF/telemetry-service/internal/models"
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

func (h *Handler) GetActivityLogs(c echo.Context) error {
	// Resolve organization from hostname or X-Organization header
	orgSlug := common.ResolveOrgFromRequest(c)
	if orgSlug == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "Could not determine organization from hostname or X-Organization header",
		})
	}

	deviceEUI := c.QueryParam("device_eui")
	if deviceEUI == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "device_eui query parameter is required",
		})
	}

	p := common.ParsePagination(c)

	activityLogs, count, err := h.tsClient.GetActivityLogs(c.Request().Context(), orgSlug, deviceEUI, p.Limit, p.Offset)
	if err != nil {
		h.logger.Error("Error occurred while fetching activity logs", zap.Error(err))
		return c.JSON(http.StatusInternalServerError, map[string]string{
			"error": "Failed to fetch activity logs",
		})
	}

	return c.JSON(http.StatusOK, map[string]interface{}{
		"message": "Get activity logs successfully",
		"logs":    activityLogs,
		"count":   count,
	})
}

func (h *Handler) InsertActivityLog(c echo.Context) error {
	// Resolve organization from hostname or X-Organization header
	orgSlug := common.ResolveOrgFromRequest(c)
	if orgSlug == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "Could not determine organization from hostname or X-Organization header",
		})
	}

	var log DeviceActivityLogRequest
	if err := c.Bind(&log); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "Invalid request body",
		})
	}

	if log.DeviceEUI == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "device_eui is required",
		})
	}

	if log.Payload == nil {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "payload is required",
		})
	}

	err := h.tsClient.InsertActivityLog(c.Request().Context(), orgSlug, models.DeviceActivityLog{
		ID:        log.ID,
		Time:      log.Time,
		DeviceEUI: log.DeviceEUI,
		Payload:   log.Payload,
	})
	if err != nil {
		h.logger.Error("Error occurred while inserting activity log", zap.Error(err))
		return c.JSON(http.StatusInternalServerError, map[string]string{
			"error": "Failed to insert activity log",
		})
	}

	return c.JSON(http.StatusOK, map[string]string{
		"message": "Activity log inserted successfully",
	})
}
