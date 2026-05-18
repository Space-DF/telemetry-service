-- migrate:up
CREATE TABLE IF NOT EXISTS device_activity_logs (
    id         VARCHAR(36) NOT NULL,
    time       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    device_eui VARCHAR(255) NOT NULL DEFAULT '',
    payload    JSONB NOT NULL DEFAULT '{}',
    CONSTRAINT device_activity_logs_pk PRIMARY KEY (time, id)
);

CREATE INDEX idx_device_activity_logs_device_eui_time
    ON device_activity_logs (device_eui, time DESC);

SELECT create_hypertable('device_activity_logs', 'time',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE,
    migrate_data => TRUE
);

ALTER TABLE device_activity_logs SET (
    timescaledb.compress = true,
    timescaledb.compress_segmentby = 'device_eui'
);

SELECT add_compression_policy('device_activity_logs', INTERVAL '7 days');
SELECT add_retention_policy('device_activity_logs', INTERVAL '30 days');

-- migrate:down
SELECT remove_retention_policy('device_activity_logs', if_exists => TRUE);
SELECT remove_compression_policy('device_activity_logs', if_exists => TRUE);
DROP TABLE IF EXISTS device_activity_logs CASCADE;