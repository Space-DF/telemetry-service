-- migrate:up
CREATE TABLE IF NOT EXISTS device_locations (
    time            TIMESTAMPTZ NOT NULL,
    device_id       VARCHAR(255) NOT NULL,
    organization_slug VARCHAR(255) NOT NULL,
    latitude        DOUBLE PRECISION NOT NULL,
    longitude       DOUBLE PRECISION NOT NULL,
    accuracy        DOUBLE PRECISION NOT NULL,
    CONSTRAINT device_locations_pk PRIMARY KEY (time, device_id)
);

CREATE INDEX idx_device_locations_org_device_time
    ON device_locations (organization_slug, device_id,
                         time DESC);

-- Create a hypertable to enable TimescaleDB features
SELECT create_hypertable('device_locations', 'time',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

ALTER TABLE device_locations
    SET (
        timescaledb.compress = true,
        timescaledb.compress_segmentby = 'organization_slug, device_id'
        );

SELECT add_compression_policy('device_locations', INTERVAL '7 days');


-- migrate:down
DROP TABLE IF EXISTS device_locations CASCADE ;
