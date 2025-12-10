-- migrate:up

DROP TABLE IF EXISTS device_locations CASCADE;

-- migrate:down

CREATE TABLE IF NOT EXISTS device_locations (
    time TIMESTAMPTZ NOT NULL,
    device_id UUID NOT NULL,
    space_slug TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    accuracy DOUBLE PRECISION,
    PRIMARY KEY (time, device_id)
);
