package activity_logs

import (
	"net/http"

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

	nextURL, previousURL := common.Paginate(count, p, common.BuildBaseURL(c), common.ExtraParams(c))

	return c.JSON(http.StatusOK, common.PaginatedResponse{
		Count:    count,
		Results:  activityLogs,
		Next:     nextURL,
		Previous: previousURL,
	})
}
