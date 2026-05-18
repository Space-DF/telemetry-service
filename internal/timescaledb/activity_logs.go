package timescaledb

import (
	"context"
	"fmt"

	"github.com/Space-DF/telemetry-service/internal/models"
	"github.com/stephenafamo/bob"
)

// GetActivityLogs retrieves activity logs for a device within a time range.
func (c *Client) GetActivityLogs(ctx context.Context, orgSlug, deviceEUI string, limit, offset int) ([]models.DeviceActivityLog, int, error) {
	GetActivityLogsCountQuery := `SELECT COUNT(*) FROM device_activity_logs WHERE device_eui = $1`

	var logs []models.DeviceActivityLog
	var total int

	err := c.WithOrgTx(ctx, orgSlug, func(txCtx context.Context, tx bob.Tx) error {
		if err := tx.QueryRowContext(txCtx,
			GetActivityLogsCountQuery,
			deviceEUI,
		).Scan(&total); err != nil {
			return fmt.Errorf("failed to count activity logs: %w", err)
		}

		rows, err := tx.QueryContext(txCtx,
			`SELECT time, id, device_eui, payload FROM device_activity_logs WHERE device_eui = $1 ORDER BY time DESC LIMIT $2 OFFSET $3`,
			deviceEUI, limit, offset,
		)
		if err != nil {
			return fmt.Errorf("failed to query activity logs: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var log models.DeviceActivityLog
			if err := rows.Scan(&log.Time, &log.ID, &log.DeviceEUI, &log.Payload); err != nil {
				return fmt.Errorf("failed to scan activity log: %w", err)
			}
			logs = append(logs, log)
		}

		return rows.Err()
	})

	if err != nil {
		return nil, 0, err
	}

	return logs, total, nil
}

// InsertActivityLog inserts a new activity log into the database.
func (c *Client) InsertActivityLog(ctx context.Context, orgSlug string, log models.DeviceActivityLog) error {
	return c.WithOrgTx(ctx, orgSlug, func(txCtx context.Context, tx bob.Tx) error {
		_, err := tx.ExecContext(txCtx,
			`INSERT INTO device_activity_logs (id, time, device_eui, payload) VALUES ($1, $2, $3, $4)`,
			log.ID, log.Time, log.DeviceEUI, log.Payload,
		)
		return err
	})
}
