package timescaledb

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	alertregistry "github.com/Space-DF/telemetry-service/internal/alerts/registry"
	"github.com/stephenafamo/bob"
)

// GetAlerts retrieves alerts for a device/category within a single day.
func (c *Client) GetAlerts(ctx context.Context, orgSlug, category, spaceSlug, deviceID, dateStr string, cautionThreshold, warningThreshold, criticalThreshold float64, page, pageSize int) ([]interface{}, int, error) {
	org := orgSlug
	if org == "" {
		org = orgFromContext(ctx)
	}
	if org == "" || spaceSlug == "" || deviceID == "" {
		return nil, 0, fmt.Errorf("org, space_slug, and device_id are required")
	}

	offset := (page - 1) * pageSize

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

	startAt, endAt, err := buildDateRange(dateStr)
	if err != nil {
		return nil, 0, err
	}

	args := []interface{}{category, spaceSlug, deviceID, startAt, endAt, pageSize, offset}
	countArgs := args[:5]

	statePredicate := processor.StatePredicate()
	if strings.TrimSpace(statePredicate) == "" {
		statePredicate = "TRUE"
	}
	whereClause := fmt.Sprintf(`
		e.is_enabled = true
		AND e.category = $1
		AND e.space_slug = $2
		AND e.device_id::text = $3
		AND %s
		AND s.reported_at >= $4
		AND s.reported_at < $5
	`, statePredicate)

	query := fmt.Sprintf(alertsQueryTemplate, whereClause)

	// Count query
	countQuery := fmt.Sprintf(alertsCountQueryTemplate, whereClause)

	var totalCount int
	var results []interface{}

	if err := c.WithOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
		// Get total count
		row := tx.QueryRowContext(txCtx, countQuery, countArgs...)
		if err := row.Scan(&totalCount); err != nil {
			return fmt.Errorf("failed to count alerts: %w", err)
		}

		// Get alerts
		rows, err := tx.QueryContext(txCtx, query, args...)
		if err != nil {
			return fmt.Errorf("failed to query alerts: %w", err)
		}
		defer func() {
			_ = rows.Close()
		}()

		for rows.Next() {
			var entityID, entityName, deviceIDVal, spaceSlugVal, state string
			var reportedAt time.Time
			var latitude, longitude sql.NullFloat64

			if err := rows.Scan(&entityID, &entityName, &deviceIDVal, &spaceSlugVal, &state, &reportedAt, &latitude, &longitude); err != nil {
				return fmt.Errorf("failed to scan alert row: %w", err)
			}

			value := 0.0
			if parsed, err := processor.ParseValue(state); err == nil {
				value = parsed
			}

			levelComputed := processor.DetermineLevel(value, cautionThreshold, warningThreshold, criticalThreshold)

			// Skip safe alerts, only return caution, warning and critical
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

			// Add location if available
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

// buildDateRange converts a required YYYY-MM-DD string into a single-day [start, end) window.
func buildDateRange(dateStr string) (time.Time, time.Time, error) {
	const layout = "2006-01-02" // Go's reference layout meaning YYYY-MM-DD
	trimmed := strings.TrimSpace(dateStr)
	if trimmed == "" {
		return time.Time{}, time.Time{}, fmt.Errorf("%w", ErrDateRequired)
	}

	// Load Vietnam timezone (UTC+7)
	vietnamTZ, err := time.LoadLocation("Asia/Ho_Chi_Minh")
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("failed to load Vietnam timezone: %w", err)
	}

	// Parse date in Vietnam timezone
	parsed, err := time.ParseInLocation(layout, trimmed, vietnamTZ)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("%w", ErrInvalidDateFormat)
	}

	start := parsed.UTC()
	end := start.Add(24 * time.Hour)
	return start, end, nil
}

const alertsQueryTemplate = `
	SELECT 
		e.id as entity_id,
		e.name as entity_name,
		e.device_id,
		e.space_slug,
		s.state,
		s.reported_at,
		loc.latitude,
		loc.longitude
	FROM entities e
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
	LIMIT $6 OFFSET $7
`

const alertsCountQueryTemplate = `
	SELECT COUNT(*) 
	FROM entities e
	INNER JOIN entity_states s ON s.entity_id = e.id
	WHERE %s
`
