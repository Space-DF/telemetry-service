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
    is_active BOOLEAN DEFAULT true,
    total_devices INT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by UUID NOT NULL
);

CREATE TABLE IF NOT EXISTS geofences (
    geofence_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    type_zone varchar(30) CHECK (type_zone IN ('safe', 'danger', 'normal')) DEFAULT 'normal',
    geometry geometry,
    is_active BOOLEAN DEFAULT true,
    space_id UUID REFERENCES spaces(space_id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Create index for geometry
CREATE INDEX IF NOT EXISTS idx_geofences_geometry ON geofences USING GIST (geometry);
CREATE INDEX IF NOT EXISTS idx_geofences_space_id ON geofences (space_id);
CREATE INDEX IF NOT EXISTS idx_geofences_type_zone ON geofences (type_zone);
CREATE INDEX IF NOT EXISTS idx_geofences_is_active ON geofences (is_active);

-- Update event_rules table schema to match API expectations
-- Add new columns needed by the API
ALTER TABLE event_rules 
ADD COLUMN IF NOT EXISTS space_id UUID REFERENCES spaces(space_id) ON DELETE SET NULL,
ADD COLUMN IF NOT EXISTS geofence_id UUID REFERENCES geofences(geofence_id) ON DELETE SET NULL,
ADD COLUMN IF NOT EXISTS definition JSON,
ADD COLUMN IF NOT EXISTS cooldown_sec INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS description TEXT;

-- Backfill defaults for new columns on existing rows
UPDATE event_rules SET cooldown_sec = 0 WHERE cooldown_sec IS NULL;

ALTER TABLE entities 
ADD COLUMN IF NOT EXISTS space_id UUID REFERENCES spaces(space_id) ON DELETE SET NULL;
ALTER TABLE events
ADD COLUMN IF NOT EXISTS space_id UUID REFERENCES spaces(space_id) ON DELETE SET NULL;

-- Remove columns that are not used by the API (safely)
ALTER TABLE event_rules 
DROP COLUMN IF EXISTS status,
DROP COLUMN IF EXISTS operator,
DROP COLUMN IF EXISTS operand,
DROP COLUMN IF EXISTS start_time,
DROP COLUMN IF EXISTS end_time;
ALTER TABLE entities 
DROP COLUMN IF EXISTS space_slug;
ALTER TABLE events 
DROP COLUMN IF EXISTS space_slug;

-- Update column name columns that are not used by the API (safely)
ALTER TABLE event_rules 
RENAME COLUMN allow_new_event TO repeat_able;

-- Backfill default for repeat_able on existing rows
UPDATE event_rules SET repeat_able = true WHERE repeat_able IS NULL;

-- Update indexes for event_rules
CREATE INDEX IF NOT EXISTS idx_event_rules_space_id ON event_rules (space_id);
CREATE INDEX IF NOT EXISTS idx_event_rules_geofence_id ON event_rules (geofence_id);

-- Add additional indexes for events table performance
CREATE INDEX IF NOT EXISTS idx_events_entity_id ON events (entity_id);
CREATE INDEX IF NOT EXISTS idx_events_space_id ON events (space_id);
CREATE INDEX IF NOT EXISTS idx_entities_space_id ON entities (space_id);

-- migrate:down

-- Revert table changes
DROP INDEX IF EXISTS idx_event_rules_space_id;
DROP INDEX IF EXISTS idx_event_rules_geofence_id;
DROP INDEX IF EXISTS idx_events_entity_id;
DROP INDEX IF EXISTS idx_events_space_id;
DROP INDEX IF EXISTS idx_entities_space_id;

-- Rename column back to original name
ALTER TABLE event_rules 
RENAME COLUMN repeat_able TO allow_new_event;

-- Add back removed columns to events table
ALTER TABLE events 
ADD COLUMN IF NOT EXISTS space_slug TEXT;

-- Add back removed columns to entities table  
ALTER TABLE entities 
ADD COLUMN IF NOT EXISTS space_slug TEXT;

-- Add back removed columns to event_rules table
ALTER TABLE event_rules 
ADD COLUMN IF NOT EXISTS status TEXT,
ADD COLUMN IF NOT EXISTS operator TEXT,
ADD COLUMN IF NOT EXISTS operand TEXT,
ADD COLUMN IF NOT EXISTS start_time TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS end_time TIMESTAMPTZ;

-- Remove added columns from events table
ALTER TABLE events 
DROP COLUMN IF EXISTS space_id;

-- Remove added columns from entities table
ALTER TABLE entities 
DROP COLUMN IF EXISTS space_id;

-- Remove added columns from event_rules table
ALTER TABLE event_rules 
DROP COLUMN IF EXISTS space_id,
DROP COLUMN IF EXISTS geofence_id,
DROP COLUMN IF EXISTS definition,
DROP COLUMN IF EXISTS cooldown_sec,
DROP COLUMN IF EXISTS description;

-- Drop geofences schema tables
DROP TABLE IF EXISTS geofences CASCADE;
DROP TABLE IF EXISTS spaces CASCADE;

-- DROP EXTENSION IF EXISTS postgis CASCADE;