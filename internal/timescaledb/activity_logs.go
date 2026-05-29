package timescaledb

import (
	"context"
	"fmt"

	"github.com/Space-DF/telemetry-service/internal/models"
	"github.com/stephenafamo/bob"
	"go.uber.org/zap"
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
		defer func() {
			if err := rows.Close(); err != nil {
				c.Logger.Error("Failed to close rows", zap.Error(err))
			}
		}()

		for rows.Next() {
			var log models.DeviceActivityLog
			if err := rows.Scan(&log.Timestamp, &log.ID, &log.DeviceEUI, &log.Payload); err != nil {
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
		result, err := tx.ExecContext(txCtx,
			`INSERT INTO device_activity_logs (id, time, device_eui, payload) VALUES ($1, $2, $3, COALESCE($4, '{}'::jsonb)) ON CONFLICT (time, id) DO NOTHING`,
			log.ID, log.Timestamp, log.DeviceEUI, log.Payload,
		)
		if err != nil {
			return err
		}
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if rowsAffected == 0 {
			c.Logger.Debug("Activity log already exists, skipping insert", zap.String("id", log.ID))
		}
		return nil
	})
}
