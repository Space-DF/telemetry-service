-- migrate:up
-- This migration creates the actions and automations tables

-- Actions Table: Defines available actions that can be triggered
CREATE TABLE IF NOT EXISTS actions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(100) NOT NULL,
    key VARCHAR(100) NOT NULL UNIQUE,
    data JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Indexes for actions
CREATE INDEX IF NOT EXISTS idx_actions_key ON actions (key);

-- Automations Table: Defines automation rules and actions
CREATE TABLE IF NOT EXISTS automations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(100) NOT NULL,
    device_id UUID NOT NULL,
    action_id UUID[],
    event_rule_id UUID REFERENCES event_rules(event_rule_id) ON DELETE CASCADE,
    space_id UUID REFERENCES spaces(space_id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Automation actions
CREATE TABLE IF NOT EXISTS automation_actions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    action_id UUID REFERENCES actions(id) ON DELETE CASCADE,
    automation_id UUID REFERENCES automations(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Remove device_id and geofence_id from event_rules
ALTER TABLE event_rules
DROP COLUMN IF EXISTS device_id,
DROP COLUMN IF EXISTS space_id,
DROP COLUMN IF EXISTS geofence_id;

-- Add event_rule_id back to geofences for reference
ALTER TABLE geofences
ADD COLUMN IF NOT EXISTS event_rule_id UUID REFERENCES event_rules(event_rule_id) ON DELETE CASCADE;

-- Add columns to events table to track source (automation or geofence)
ALTER TABLE events
ADD COLUMN IF NOT EXISTS location JSONB,
ADD COLUMN IF NOT EXISTS automation_id UUID,
ADD COLUMN IF NOT EXISTS device_id UUID,
ADD COLUMN IF NOT EXISTS geofence_id UUID,
ADD COLUMN IF NOT EXISTS title VARCHAR(100);

-- Indexes for automations
CREATE INDEX IF NOT EXISTS idx_automations_device_id ON automations (device_id);
CREATE INDEX IF NOT EXISTS idx_automations_event_rule_id ON automations (event_rule_id);
CREATE INDEX IF NOT EXISTS idx_automations_space_id ON automations (space_id);

-- Index for fast device event lookups
CREATE INDEX IF NOT EXISTS idx_events_device_id ON events (device_id);

-- First drop the FK constraint from entity_states that references events.event_id
-- Then drop the event_id column itself
ALTER TABLE entity_states
DROP CONSTRAINT IF EXISTS entity_states_event_id_fkey;

ALTER TABLE entity_states
DROP COLUMN IF EXISTS event_id;

-- Remove data_id, trigger_id column from events table
ALTER TABLE events 
DROP COLUMN IF EXISTS trigger_id,
DROP COLUMN IF EXISTS data_id;

-- Drop event_data table
DROP TABLE IF EXISTS event_data CASCADE;

-- migrate:down
-- Drop automations schema tables
DROP TABLE IF EXISTS automations CASCADE;
DROP TABLE IF EXISTS actions CASCADE;
DROP TABLE IF EXISTS automation_actions CASCADE;

-- Add device_id and geofence_id back to event_rules
ALTER TABLE event_rules
ADD COLUMN IF NOT EXISTS device_id UUID,
ADD COLUMN IF NOT EXISTS space_id UUID,
ADD COLUMN IF NOT EXISTS geofence_id UUID;

-- Remove event_rule_id from geofences
ALTER TABLE geofences
DROP COLUMN IF EXISTS event_rule_id;

-- Remove automation_id and geofence_id from events
ALTER TABLE events
DROP COLUMN IF EXISTS location,
DROP COLUMN IF EXISTS automation_id,
DROP COLUMN IF EXISTS device_id,
DROP COLUMN IF EXISTS geofence_id,
ADD COLUMN IF NOT EXISTS trigger_id UUID,
DROP COLUMN IF EXISTS title;

-- Re-create event_data table
CREATE TABLE IF NOT EXISTS event_data (
    data_id SERIAL PRIMARY KEY,
    hash BIGINT NOT NULL UNIQUE,
    shared_data JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_event_data_hash ON event_data (hash);

-- Re-add data_id column to events
ALTER TABLE events ADD COLUMN IF NOT EXISTS data_id INTEGER REFERENCES event_data(data_id) ON DELETE CASCADE;
CREATE INDEX IF NOT EXISTS idx_events_data_id ON events (data_id);

-- Re-add event_id column to entity_states and re-add foreign key constraint
ALTER TABLE entity_states
ADD COLUMN IF NOT EXISTS event_id UUID;

ALTER TABLE entity_states
ADD CONSTRAINT entity_states_event_id_fkey
FOREIGN KEY (event_id) REFERENCES events(event_id) ON DELETE SET NULL;