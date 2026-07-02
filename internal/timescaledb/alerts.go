package timescaledb

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	alertregistry "github.com/Space-DF/telemetry-service/internal/alerts/registry"
	"github.com/Space-DF/telemetry-service/internal/api/common"
	"github.com/stephenafamo/bob"
)

// GetAlerts retrieves alerts for a device/category within a time range.
func (c *Client) GetAlerts(ctx context.Context, orgSlug, category, deviceID, startStr, endStr string, cautionThreshold, warningThreshold, criticalThreshold float64, limit, offset int) ([]interface{}, int, error) {
	org := orgSlug
	if org == "" {
		org = orgFromContext(ctx)
	}
	if org == "" || deviceID == "" {
		return nil, 0, fmt.Errorf("org and device_id are required")
	}

	if limit <= 0 {
		limit = common.DefaultLimit
	}
	if offset < 0 {
		offset = 0
	}

	processor, ok := alertregistry.Get(category)
	if !ok {
		return nil, 0, fmt.Errorf("unsupported category: %s", category)
	}

	// Apply processor defaults if not provided
	if cautionThreshold <= 0 {
		cautionThreshold = processor.DefaultCautionThreshold()
	}
	if warningThreshold <= 0 {
		warningThreshold = processor.DefaultWarningThreshold()
	}
	if criticalThreshold <= 0 {
		criticalThreshold = processor.DefaultCriticalThreshold()
	}

	startAt, endAt, err := buildDateRange(startStr, endStr)
	if err != nil {
		return nil, 0, err
	}

	results, totalCount, err := c.queryAlerts(ctx, org, processor, category, deviceID, startAt, endAt, cautionThreshold, warningThreshold, criticalThreshold, limit, offset)
	if err != nil {
		return nil, 0, err
	}

	return results, totalCount, nil
}

func (c *Client) queryAlerts(ctx context.Context, org string, processor alertregistry.Processor, category, deviceID string, startAt, endAt time.Time, cautionThreshold, warningThreshold, criticalThreshold float64, limit, offset int) ([]interface{}, int, error) {
	args := []interface{}{category, deviceID, startAt, endAt, limit, offset}
	countArgs := args[:4]

	whereClause := `
		e.is_enabled = true
		AND e.category = $1
		AND e.device_id::text = $2
		AND TRUE
		AND s.reported_at >= $3
		AND s.reported_at <= $4
	`
	query := fmt.Sprintf(alertsQueryTemplate, whereClause, len(args)-1, len(args))
	countQuery := fmt.Sprintf(alertsCountQueryTemplate, whereClause)

	var totalCount int
	var results []interface{}

	if err := c.WithOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
		row := tx.QueryRowContext(txCtx, countQuery, countArgs...)
		if err := row.Scan(&totalCount); err != nil {
			return fmt.Errorf("failed to count alerts: %w", err)
		}

		rows, err := tx.QueryContext(txCtx, query, args...)
		if err != nil {
			return fmt.Errorf("failed to query alerts: %w", err)
		}
		defer func() {
			_ = rows.Close()
		}()

		for rows.Next() {
			var entityID, entityName, deviceIDVal, state string
			var reportedAt time.Time
			var spaceSlugVal sql.NullString
			var latitude, longitude sql.NullFloat64

			if err := rows.Scan(&entityID, &entityName, &deviceIDVal, &spaceSlugVal, &state, &reportedAt, &latitude, &longitude); err != nil {
				return fmt.Errorf("failed to scan alert row: %w", err)
			}

			value := 0.0
			if parsed, err := processor.ParseValue(state); err == nil {
				value = parsed
			}

			levelComputed := processor.DetermineLevel(value, cautionThreshold, warningThreshold, criticalThreshold)
			if levelComputed == "safe" {
				continue
			}

			alert := map[string]interface{}{
				"id":                 entityID,
				"type":               processor.DetermineType(value, cautionThreshold, warningThreshold, criticalThreshold),
				"level":              levelComputed,
				"message":            processor.GenerateMessage(levelComputed, value),
				"entity_id":          entityID,
				"entity_name":        entityName,
				"device_id":          deviceIDVal,
				"space_slug":         spaceSlugVal,
				processor.ValueKey(): value,
				"unit":               processor.Unit(),
				"threshold": map[string]interface{}{
					"caution":  cautionThreshold,
					"warning":  warningThreshold,
					"critical": criticalThreshold,
				},
				"reported_at": reportedAt,
			}

			if latitude.Valid && longitude.Valid {
				alert["location"] = map[string]interface{}{
					"latitude":  latitude.Float64,
					"longitude": longitude.Float64,
				}
			}

			results = append(results, alert)
		}

		if err := rows.Err(); err != nil {
			return fmt.Errorf("error iterating alert rows: %w", err)
		}

		return nil
	}); err != nil {
		return nil, 0, err
	}

	return results, totalCount, nil
}

func buildDateRange(startStr, endStr string) (time.Time, time.Time, error) {
	const dateLayout = "2006-01-02"
	startTime := strings.TrimSpace(startStr)
	endTime := strings.TrimSpace(endStr)

	if startTime == "" {
		return time.Time{}, time.Time{}, fmt.Errorf("%w", ErrDateRequired)
	}

	var start time.Time
	var err error

	start, err = time.Parse(time.RFC3339, startTime)
	if err != nil {
		parsed, pErr := time.ParseInLocation(dateLayout, startTime, time.UTC)
		if pErr != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("%w", ErrInvalidDateFormat)
		}
		start = parsed.UTC()
	}

	var end time.Time
	if endTime == "" {
		end = time.Now().UTC()
	} else {
		end, err = time.Parse(time.RFC3339, endTime)
		if err != nil {
			parsed, pErr := time.ParseInLocation(dateLayout, endTime, time.UTC)
			if pErr != nil {
				return time.Time{}, time.Time{}, fmt.Errorf("%w", ErrInvalidDateFormat)
			}
			end = parsed.UTC().Add(24 * time.Hour)
		}
	}

	if !start.Before(end) {
		return time.Time{}, time.Time{}, fmt.Errorf("start must be before end")
	}
	return start.UTC(), end.UTC(), nil
}

const alertsQueryTemplate = `
	SELECT 
		e.id as entity_id,
		e.name as entity_name,
		e.device_id,
		sp.space_slug,
		s.state,
		s.reported_at,
		loc.latitude,
		loc.longitude
	FROM entities e
	LEFT JOIN spaces sp ON e.space_id = sp.space_id
	INNER JOIN entity_states s ON s.entity_id = e.id
	LEFT JOIN LATERAL (
		SELECT 
			(a.shared_attrs->>'latitude')::float as latitude,
			(a.shared_attrs->>'longitude')::float as longitude
		FROM entities e2
		INNER JOIN entity_states s2 ON s2.entity_id = e2.id
		LEFT JOIN entity_state_attributes a ON a.id = s2.attributes_id
		WHERE e2.device_id = e.device_id
			AND e2.category = 'location'
			AND e2.is_enabled = true
			AND a.shared_attrs ? 'latitude'
			AND a.shared_attrs ? 'longitude'
		ORDER BY s2.reported_at DESC
		LIMIT 1
	) loc ON true
	WHERE %s
	ORDER BY s.reported_at DESC
	LIMIT $%d OFFSET $%d
`

const alertsCountQueryTemplate = `
	SELECT COUNT(*) 
	FROM entities e
	LEFT JOIN spaces sp ON e.space_id = sp.space_id
	INNER JOIN entity_states s ON s.entity_id = e.id
	WHERE %s
`
