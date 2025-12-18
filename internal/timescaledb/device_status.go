package timescaledb

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/stephenafamo/bob"
	"github.com/stephenafamo/scan"
)

type DeviceStatusSnapshot struct {
	DeviceID  string
	SpaceSlug string
	Entities  []DeviceEntitySnapshot
}

type DeviceEntitySnapshot struct {
	EntityID          string
	Category          string
	UniqueKey         string
	Name              string
	UnitOfMeasurement string
	State             string
	ReportedAt        time.Time
}

func (c *Client) GetDeviceStatusSnapshot(ctx context.Context, deviceID, spaceSlug string) (*DeviceStatusSnapshot, error) {
	if deviceID == "" || spaceSlug == "" {
		return nil, fmt.Errorf("device_id and space_slug are required")
	}

	snapshot := &DeviceStatusSnapshot{
		DeviceID:  deviceID,
		SpaceSlug: spaceSlug,
	}

	org := orgFromContext(ctx)
	if org == "" {
		return nil, fmt.Errorf("organization not found in context")
	}

	if err := c.withOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
		return c.fillDeviceStatusSnapshot(txCtx, tx, snapshot)
	}); 
	
	err != nil {
		return nil, err
	}

	return snapshot, nil
}

func (c *Client) fillDeviceStatusSnapshot(ctx context.Context, q scan.Queryer, snapshot *DeviceStatusSnapshot) error {
	rows, err := q.QueryContext(ctx, latestEntityStatesQuery, snapshot.DeviceID, snapshot.SpaceSlug)
	if err != nil {
		return fmt.Errorf("fetch entity states: %w", err)
	}
	defer func() {
		_ = rows.Close()
	}()

	for rows.Next() {
		var entityID, category, uniqueKey, name, unit, state sql.NullString
		var reportedAt sql.NullTime
		if err := rows.Scan(&entityID, &category, &uniqueKey, &name, &unit, &state, &reportedAt); err != nil {
			return fmt.Errorf("scan entity state: %w", err)
		}
		if !reportedAt.Valid {
			continue
		}
		snapshot.Entities = append(snapshot.Entities, DeviceEntitySnapshot{
			EntityID:          entityID.String,
			Category:          category.String,
			UniqueKey:         uniqueKey.String,
			Name:              name.String,
			UnitOfMeasurement: unit.String,
			State:             state.String,
			ReportedAt:        reportedAt.Time,
		})
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate entity states: %w", err)
	}

	return nil
}

const latestEntityStatesQuery = `
	SELECT DISTINCT ON (e.id)
		e.id::text,
		COALESCE(e.category, ''),
		COALESCE(e.unique_key, ''),
		COALESCE(e.name, ''),
		COALESCE(e.unit_of_measurement, ''),
		s.state,
		s.reported_at
	FROM entities e
	JOIN entity_states s ON s.entity_id = e.id
	WHERE e.device_id::text = $1
		AND e.space_slug = $2
		AND e.is_enabled = true
	ORDER BY e.id, s.reported_at DESC
`
