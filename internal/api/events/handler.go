package events

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Space-DF/telemetry-service/internal/api/common"
	"github.com/Space-DF/telemetry-service/internal/config"
	"github.com/Space-DF/telemetry-service/internal/timescaledb"
	"github.com/labstack/echo/v4"
	"go.uber.org/zap"
)

type deviceStatusResponse struct {
	Organization string          `json:"organization"`
	DeviceID     string          `json:"device_id"`
	SpaceSlug    string          `json:"space_slug"`
	Events       []eventResponse `json:"events"`
}

const (
	eventKindSystem    = "system"
	eventKindThreshold = "threshold"
)

type eventResponse struct {
	Kind       string    `json:"kind"`
	Type       string    `json:"type"`
	Category   string    `json:"category"`
	Unit       string    `json:"unit,omitempty"`
	Threshold  float64   `json:"threshold,omitempty"`
	Value      *float64  `json:"value,omitempty"`
	ReportedAt time.Time `json:"reported_at"`
	Message    string    `json:"message,omitempty"`
	EntityID   string    `json:"entity_id"`
	UniqueKey  string    `json:"unique_key"`
	Name       string    `json:"name"`
}

type eventMatch struct {
	Rule   config.DeviceEventRule
	Entity timescaledb.DeviceEntitySnapshot
	Value  float64
	Low    bool
}

func getDeviceStatus(logger *zap.Logger, tsClient *timescaledb.Client, rules []config.DeviceEventRule) echo.HandlerFunc {
	return func(c echo.Context) error {
		deviceID := strings.TrimSpace(c.QueryParam("device_id"))
		if deviceID == "" {
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": "device_id is required",
			})
		}

		spaceSlug, err := common.ResolveSpaceSlugFromRequest(c)
		if err != nil {
			return err
		}

		orgToUse := common.ResolveOrgFromRequest(c)
		if orgToUse == "" {
			return c.JSON(http.StatusBadRequest, map[string]string{
				"error": "Could not determine organization from hostname or X-Organization header",
			})
		}

		logger.Info("Selecting DB schema for device status request",
			zap.String("org_used", orgToUse),
			zap.String("device_id", deviceID),
			zap.String("space_slug", spaceSlug),
		)

		ctx := timescaledb.ContextWithOrg(c.Request().Context(), orgToUse)
		snapshot, err := tsClient.GetDeviceStatusSnapshot(ctx, deviceID, spaceSlug)
		if err != nil {
			logger.Error("failed to query device status", zap.Error(err))
			return c.JSON(http.StatusInternalServerError, map[string]string{
				"error": "failed to query device status",
			})
		}

		response := buildDeviceStatusResponse(snapshot, rules)
		response.Organization = orgToUse
		return c.JSON(http.StatusOK, response)
	}
}

func buildDeviceStatusResponse(snapshot *timescaledb.DeviceStatusSnapshot, rules []config.DeviceEventRule) deviceStatusResponse {
	response := deviceStatusResponse{
		DeviceID:  snapshot.DeviceID,
		SpaceSlug: snapshot.SpaceSlug,
	}

	matches := matchEventRules(snapshot.Entities, rules)
	response.Events = buildEvents(matches, snapshot.Entities)
	if len(response.Events) == 0 {
		response.Events = []eventResponse{}
	}

	return response
}

func matchEventRules(entities []timescaledb.DeviceEntitySnapshot, rules []config.DeviceEventRule) []eventMatch {
	if len(entities) == 0 || len(rules) == 0 {
		return nil
	}

	latestByCategory := make(map[string]timescaledb.DeviceEntitySnapshot, len(entities))
	latestByCategoryUnit := make(map[string]timescaledb.DeviceEntitySnapshot, len(entities))

	for i := range entities {
		ent := entities[i]
		categoryKey := normalizeCategory(ent.Category)
		if categoryKey == "" {
			continue
		}
		unitKey := normalizeUnit(ent.UnitOfMeasurement)
		catUnitKey := categoryKey + "|" + unitKey

		if cur, ok := latestByCategory[categoryKey]; !ok || ent.ReportedAt.After(cur.ReportedAt) {
			latestByCategory[categoryKey] = ent
		}
		if cur, ok := latestByCategoryUnit[catUnitKey]; !ok || ent.ReportedAt.After(cur.ReportedAt) {
			latestByCategoryUnit[catUnitKey] = ent
		}
	}

	matches := make([]eventMatch, 0, len(rules))
	for _, rule := range rules {
		categoryKey := normalizeCategory(rule.Category)
		if categoryKey == "" {
			continue
		}
		unitKey := normalizeUnit(rule.Unit)

		var ent timescaledb.DeviceEntitySnapshot
		var ok bool
		if unitKey != "" {
			ent, ok = latestByCategoryUnit[categoryKey+"|"+unitKey]
		} else {
			ent, ok = latestByCategory[categoryKey]
		}
		if !ok {
			continue
		}

		val, ok := parseFloatValue(ent.State)
		if !ok {
			continue
		}

		low := val <= rule.LowThreshold
		matches = append(matches, eventMatch{
			Rule:   rule,
			Entity: ent,
			Value:  val,
			Low:    low,
		})
	}

	return matches
}

func buildEvents(matches []eventMatch, entities []timescaledb.DeviceEntitySnapshot) []eventResponse {
	if len(matches) == 0 {
		return buildLatestMessageEvent(entities)
	}

	results := make([]eventResponse, 0, len(matches))
	if latest := buildLatestMessageEvent(entities); len(latest) > 0 {
		results = append(results, latest...)
	}

	for _, match := range matches {
		if !match.Low {
			continue
		}
		value := match.Value
		category := strings.TrimSpace(match.Rule.Category)
		eventType := strings.ToLower(category) + "_low"
		results = append(results, eventResponse{
			Kind:       eventKindThreshold,
			Type:       eventType,
			Category:   match.Rule.Category,
			Unit:       strings.TrimSpace(match.Rule.Unit),
			Threshold:  match.Rule.LowThreshold,
			Value:      &value,
			ReportedAt: match.Entity.ReportedAt,
			EntityID:   match.Entity.EntityID,
			UniqueKey:  match.Entity.UniqueKey,
			Name:       match.Entity.Name,
		})
	}

	return results
}

func buildLatestMessageEvent(entities []timescaledb.DeviceEntitySnapshot) []eventResponse {
	if len(entities) == 0 {
		return nil
	}

	latestIdx := 0
	for i := range entities {
		if entities[i].ReportedAt.After(entities[latestIdx].ReportedAt) {
			latestIdx = i
		}
	}
	latest := entities[latestIdx]
	message := "latest message was at " + latest.ReportedAt.UTC().Format("2006-01-02 15:04:05Z07:00")

	return []eventResponse{
		{
			Kind:       eventKindSystem,
			Type:       "latest_message",
			Category:   latest.Category,
			Unit:       strings.TrimSpace(latest.UnitOfMeasurement),
			ReportedAt: latest.ReportedAt,
			Message:    message,
			EntityID:   latest.EntityID,
			UniqueKey:  latest.UniqueKey,
			Name:       latest.Name,
		},
	}
}

func parseFloatValue(state string) (float64, bool) {
	trimmed := strings.TrimSpace(state)
	if trimmed == "" {
		return 0, false
	}
	val, err := strconv.ParseFloat(trimmed, 64)
	if err != nil {
		return 0, false
	}
	return val, true
}

func normalizeUnit(unit string) string {
	return strings.ToLower(strings.TrimSpace(unit))
}

func normalizeCategory(category string) string {
	return strings.ToLower(strings.TrimSpace(category))
}
