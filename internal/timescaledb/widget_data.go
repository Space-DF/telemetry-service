package timescaledb

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/stephenafamo/bob"
)

// EntityDataPoint represents a data point for aggregation
type EntityDataPoint struct {
	Timestamp time.Time
	Value     float64
}

// HistogramBucketData represents histogram bucket data
type HistogramBucketData struct {
	Bucket string
	Count  int64
	Value  float64
}

// TableDataRow represents a row in table data
type TableDataRow struct {
	Timestamp time.Time
	Values    map[string]interface{}
}

// GetLatestEntityValue gets the latest numeric value for an entity
func (c *Client) GetLatestEntityValue(ctx context.Context, entityID string) (float64, string, error) {
	var value float64
	var unitOfMeasurement string

	org := orgFromContext(ctx)
	if org == "" {
		return 0.0, "", fmt.Errorf("organization not found in context")
	}

	err := c.withOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
		row := tx.QueryRowContext(txCtx, `
			SELECT COALESCE(es.state::float8, 0), COALESCE(e.unit_of_measurement, '')
			FROM entity_states es
			JOIN entities e ON es.entity_id = e.id
			WHERE e.unique_key = $1
			ORDER BY es.reported_at DESC
			LIMIT 1
		`, entityID)

		if err := row.Scan(&value, &unitOfMeasurement); err != nil {
			if err.Error() == "sql: no rows in result set" {
				return nil
			}
			return fmt.Errorf("scan row: %w", err)
		}

		return nil
	})

	if err != nil {
		return 0.0, "", err
	}

	return value, unitOfMeasurement, nil
}

// GetLatestEntityBoolValue gets the latest boolean value for an entity
func (c *Client) GetLatestEntityBoolValue(ctx context.Context, entityID string) (bool, error) {
	var state string

	org := orgFromContext(ctx)
	if org == "" {
		return false, fmt.Errorf("organization not found in context")
	}

	err := c.withOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
		row := tx.QueryRowContext(txCtx, `
			SELECT es.state
			FROM entity_states es
			JOIN entities e ON es.entity_id = e.id
			WHERE e.unique_key = $1
			ORDER BY es.reported_at DESC
			LIMIT 1
		`, entityID)

		if err := row.Scan(&state); err != nil {
			if err.Error() == "sql: no rows in result set" {
				return nil
			}
			return fmt.Errorf("scan row: %w", err)
		}

		return nil
	})

	if err != nil {
		return false, err
	}

	// Parse state as boolean
	return state == "true" || state == "on" || state == "1", nil
}

// GetAggregatedEntityData gets aggregated data for a time range grouped by the specified period
func (c *Client) GetAggregatedEntityData(
	ctx context.Context,
	entityID string,
	startTime, endTime time.Time,
	groupBy string,
) ([]EntityDataPoint, error) {
	var dataPoints []EntityDataPoint

	org := orgFromContext(ctx)
	if org == "" {
		return nil, fmt.Errorf("organization not found in context")
	}

	var timeBucket string

	// Determine time bucket based on groupBy parameter
	switch groupBy {
	case "hour":
		timeBucket = "1 hour"
	case "day":
		timeBucket = "1 day"
	case "week":
		timeBucket = "1 week"
	case "month":
		timeBucket = "1 month"
	default:
		timeBucket = "1 day"
	}

	err := c.withOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
		query := fmt.Sprintf(`
			SELECT 
				time_bucket('%s', es.reported_at) as bucket_time,
				AVG(COALESCE(es.state::float8, 0)) as avg_value
			FROM entity_states es
			JOIN entities e ON es.entity_id = e.id
			WHERE e.unique_key = $1 
				AND es.reported_at BETWEEN $2 AND $3
			GROUP BY bucket_time
			ORDER BY bucket_time ASC
		`, timeBucket)

		rows, err := tx.QueryContext(txCtx, query, entityID, startTime, endTime)
		if err != nil {
			return fmt.Errorf("query aggregated data: %w", err)
		}
		defer func() {
			if err := rows.Close(); err != nil {
				log.Printf("error closing rows: %v", err)
			}
		}()

		for rows.Next() {
			var bucketTime time.Time
			var value float64

			if err := rows.Scan(&bucketTime, &value); err != nil {
				return fmt.Errorf("scan aggregated row: %w", err)
			}

			dataPoints = append(dataPoints, EntityDataPoint{
				Timestamp: bucketTime,
				Value:     value,
			})
		}

		if err = rows.Err(); err != nil {
			return fmt.Errorf("row iteration error: %w", err)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return dataPoints, nil
}

// GetHistogramData gets histogram bucket data for an entity
func (c *Client) GetHistogramData(
	ctx context.Context,
	entityID string,
	startTime, endTime time.Time,
	groupBy string,
) ([]HistogramBucketData, error) {
	var values []float64

	org := orgFromContext(ctx)
	if org == "" {
		return nil, fmt.Errorf("organization not found in context")
	}

	// Get all values in range
	err := c.withOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
		query := `
			SELECT COALESCE(es.state::float8, 0)
			FROM entity_states es
			JOIN entities e ON es.entity_id = e.id
			WHERE e.unique_key = $1 
				AND es.reported_at BETWEEN $2 AND $3
			ORDER BY es.state::float8
		`

		rows, err := tx.QueryContext(txCtx, query, entityID, startTime, endTime)
		if err != nil {
			return fmt.Errorf("query histogram data: %w", err)
		}
		defer func() {
			if err := rows.Close(); err != nil {
				log.Printf("error closing rows: %v", err)
			}
		}()

		for rows.Next() {
			var value float64
			if err := rows.Scan(&value); err != nil {
				return fmt.Errorf("scan histogram value: %w", err)
			}
			values = append(values, value)
		}

		if err = rows.Err(); err != nil {
			return fmt.Errorf("row iteration error: %w", err)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	if len(values) == 0 {
		return []HistogramBucketData{}, nil
	}

	// Calculate min, max and bucket size
	minVal := values[0]
	maxVal := values[0]
	for _, v := range values {
		if v < minVal {
			minVal = v
		}
		if v > maxVal {
			maxVal = v
		}
	}

	// Create 5 buckets
	numBuckets := 5
	bucketSize := (maxVal - minVal) / float64(numBuckets)
	if bucketSize == 0 {
		bucketSize = 1
	}

	// Initialize buckets
	buckets := make([]HistogramBucketData, numBuckets)
	bucketCounts := make([]int64, numBuckets)

	for i := 0; i < numBuckets; i++ {
		bucketStart := minVal + float64(i)*bucketSize
		bucketEnd := minVal + float64(i+1)*bucketSize
		buckets[i] = HistogramBucketData{
			Bucket: fmt.Sprintf("%.1f-%.1f", bucketStart, bucketEnd),
			Count:  0,
			Value:  (bucketStart + bucketEnd) / 2,
		}
	}

	// Count values in each bucket
	for _, v := range values {
		bucketIndex := int((v - minVal) / bucketSize)
		if bucketIndex >= numBuckets {
			bucketIndex = numBuckets - 1
		}
		bucketCounts[bucketIndex]++
	}

	for i := 0; i < numBuckets; i++ {
		buckets[i].Count = bucketCounts[i]
	}

	return buckets, nil
}

// GetTableData gets raw entity data for table display
func (c *Client) GetTableData(
	ctx context.Context,
	entityID string,
	startTime, endTime time.Time,
) ([]TableDataRow, []string, error) {
	var tableRows []TableDataRow
	columns := []string{"timestamp", "state"}

	org := orgFromContext(ctx)
	if org == "" {
		return nil, nil, fmt.Errorf("organization not found in context")
	}

	err := c.withOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
		query := `
			SELECT 
				es.reported_at,
				es.state,
				es.attributes_id,
				COALESCE(esa.shared_attrs, '{}'::jsonb) as attributes
			FROM entity_states es
			JOIN entities e ON es.entity_id = e.id
			LEFT JOIN entity_state_attributes esa ON es.attributes_id = esa.id
			WHERE e.unique_key = $1 
				AND es.reported_at BETWEEN $2 AND $3
			ORDER BY es.reported_at DESC
			LIMIT 1000
		`

		rows, err := tx.QueryContext(txCtx, query, entityID, startTime, endTime)
		if err != nil {
			return fmt.Errorf("query table data: %w", err)
		}
		defer func() {
			if err := rows.Close(); err != nil {
				log.Printf("error closing rows: %v", err)
			}
		}()

		for rows.Next() {
			var timestamp time.Time
			var state string
			var attributesID interface{}
			var attributes string

			if err := rows.Scan(&timestamp, &state, &attributesID, &attributes); err != nil {
				return fmt.Errorf("scan table row: %w", err)
			}

			values := map[string]interface{}{
				"timestamp": timestamp,
				"state":     state,
			}

			// Parse attributes if present
			if attributes != "" && attributes != "{}" {
				values["attributes"] = attributes
				// Add to columns if not already there
				hasAttr := false
				for _, col := range columns {
					if col == "attributes" {
						hasAttr = true
						break
					}
				}
				if !hasAttr {
					columns = append(columns, "attributes")
				}
			}

			tableRows = append(tableRows, TableDataRow{
				Timestamp: timestamp,
				Values:    values,
			})
		}

		if err = rows.Err(); err != nil {
			return fmt.Errorf("row iteration error: %w", err)
		}

		return nil
	})

	if err != nil {
		return nil, nil, err
	}

	return tableRows, columns, nil
}
