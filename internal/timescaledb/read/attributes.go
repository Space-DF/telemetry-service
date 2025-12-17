package read

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Space-DF/telemetry-service/internal/timescaledb/core"
	"github.com/stephenafamo/bob"
)

// GetLatestAttributesForDeviceAt returns shared attributes for a device at or before the given time.
func (s *Service) GetLatestAttributesForDeviceAt(ctx context.Context, deviceID string, at time.Time) (map[string]interface{}, error) {
	org := core.OrgFromContext(ctx)

	query := `SELECT a.shared_attrs
		FROM entities e
		JOIN entity_states s ON s.entity_id = e.id
		LEFT JOIN entity_state_attributes a ON s.attributes_id = a.id
		WHERE e.device_id::text = $1 AND s.reported_at <= $2 AND a.shared_attrs IS NOT NULL
		ORDER BY s.reported_at DESC
		LIMIT 1`

	var rawAttrs []byte
	if org != "" {
		if err := s.base.WithOrgTx(ctx, org, func(txCtx context.Context, tx bob.Tx) error {
			rows, err := tx.QueryContext(txCtx, query, deviceID, at)
			if err != nil {
				return err
			}
			defer func() { _ = rows.Close() }()
			if rows.Next() {
				return rows.Scan(&rawAttrs)
			}
			return nil
		}); err != nil {
			return nil, fmt.Errorf("failed to query attributes: %w", err)
		}
	} else {
		rows, err := s.base.DB().QueryContext(ctx, query, deviceID, at)
		if err != nil {
			return nil, fmt.Errorf("failed to query attributes: %w", err)
		}
		defer func() { _ = rows.Close() }()
		if rows.Next() {
			if err := rows.Scan(&rawAttrs); err != nil {
				return nil, err
			}
		}
	}

	if len(rawAttrs) == 0 {
		return nil, nil
	}

	var attrs map[string]interface{}
	if err := json.Unmarshal(rawAttrs, &attrs); err != nil {
		return nil, fmt.Errorf("failed to unmarshal attributes JSON: %w", err)
	}

	return attrs, nil
}
