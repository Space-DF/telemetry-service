-- migrate:up
-- Enable PostGIS extension for geometry types
CREATE EXTENSION IF NOT EXISTS postgis;

-- Geofences Schema
CREATE TABLE IF NOT EXISTS spaces (
    space_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    logo TEXT,
    space_slug TEXT NOT NULL,
    description TEXT,
    is_active BOOLEAN DEFAULT false,
    total_devices INT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by UUID NOT NULL
);

CREATE TABLE IF NOT EXISTS geofences (
    geofence_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    type_zone varchar(30) CHECK (type_zone IN ('safe', 'danger', 'normal')) DEFAULT 'normal',
    geometry geometry,
    is_active BOOLEAN DEFAULT false,
    space_id UUID REFERENCES spaces(space_id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Create index for geometry
CREATE INDEX IF NOT EXISTS idx_geofences_geometry ON geofences USING GIST (geometry);
CREATE INDEX IF NOT EXISTS idx_geofences_space_id ON geofences (space_id);
CREATE INDEX IF NOT EXISTS idx_geofences_type_zone ON geofences (type_zone);
CREATE INDEX IF NOT EXISTS idx_geofences_is_active ON geofences (is_active);

-- migrate:down

-- Drop geofences schema tables
DROP TABLE IF EXISTS geofences CASCADE;
DROP TABLE IF EXISTS spaces CASCADE;

-- DROP EXTENSION IF EXISTS postgis CASCADE;