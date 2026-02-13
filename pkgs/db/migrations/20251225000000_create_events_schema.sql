-- migrate:up
-- Events Schema
CREATE TABLE IF NOT EXISTS event_types (
    event_type_id SERIAL PRIMARY KEY,
    event_type TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Insert common event types
INSERT INTO event_types (event_type) VALUES
    ('state_changed'),
    ('automation_triggered'),
    ('device_event'),
    ('user_action')
ON CONFLICT (event_type) DO NOTHING;

-- Event Data: Shared data deduplicated by hash
CREATE TABLE IF NOT EXISTS event_data (
    data_id SERIAL PRIMARY KEY,
    hash BIGINT NOT NULL UNIQUE,
    shared_data JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_event_data_hash ON event_data (hash);

-- Event Rules: Rules for triggering events based on conditions
CREATE TABLE IF NOT EXISTS event_rules (
    event_rule_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    device_id UUID, -- Device-specific automation rules
    rule_key TEXT NOT NULL, -- e.g., 'battery_low', 'temperature_low'
    operator VARCHAR(16) CHECK (operator IN ('eq', 'ne', 'gt', 'lt', 'gte', 'lte', 'contains')),
    operand TEXT NOT NULL,
    status VARCHAR(16) CHECK (status IN ('active', 'inactive', 'paused')) DEFAULT 'active',
    is_active BOOLEAN DEFAULT true,
    start_time TIMESTAMPTZ,
    end_time TIMESTAMPTZ,
    allow_new_event BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Indexes for event_rules
CREATE INDEX IF NOT EXISTS idx_event_rules_device_id ON event_rules (device_id);
CREATE INDEX IF NOT EXISTS idx_event_rules_status ON event_rules (status);
CREATE INDEX IF NOT EXISTS idx_event_rules_is_active ON event_rules (is_active);
-- Composite index for active device rules query
CREATE INDEX IF NOT EXISTS idx_event_rules_active_device ON event_rules (is_active, device_id, created_at DESC)
WHERE is_active = true;
CREATE INDEX IF NOT EXISTS idx_event_rules_time_range ON event_rules (start_time, end_time)
WHERE is_active = true;

-- Events: Event occurrences linking to event_type and event_data
-- space_slug is stored here for filtering events within a space
-- entity_id links the event to a specific entity for faster queries
CREATE TABLE IF NOT EXISTS events (
    event_id BIGSERIAL PRIMARY KEY,
    event_type_id INTEGER NOT NULL REFERENCES event_types(event_type_id) ON DELETE CASCADE,
    data_id INTEGER REFERENCES event_data(data_id) ON DELETE SET NULL,
    event_level TEXT CHECK (event_level IN ('manufacturer', 'system', 'automation')),
    event_rule_id UUID REFERENCES event_rules(event_rule_id) ON DELETE SET NULL,
    space_slug TEXT,
    entity_id TEXT,
    state_id UUID REFERENCES entity_states(id) ON DELETE SET NULL,
    trigger_id UUID, -- for future automation scale
    time_fired_ts BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_events_event_type_id ON events (event_type_id);
CREATE INDEX IF NOT EXISTS idx_events_event_rule_id ON events (event_rule_id);
CREATE INDEX IF NOT EXISTS idx_events_space_slug ON events (space_slug);
CREATE INDEX IF NOT EXISTS idx_events_entity_id ON events (entity_id);
CREATE INDEX IF NOT EXISTS idx_events_state_id ON events (state_id);
CREATE INDEX IF NOT EXISTS idx_events_trigger_id ON events (trigger_id);
CREATE INDEX IF NOT EXISTS idx_events_time_fired_ts ON events (time_fired_ts DESC);
CREATE INDEX IF NOT EXISTS idx_events_data_id ON events (data_id);

-- Add event_id column to entity_states to create bidirectional linkage
-- This allows states to reference their triggering event
ALTER TABLE entity_states ADD COLUMN IF NOT EXISTS event_id BIGINT REFERENCES events(event_id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_entity_states_event_id ON entity_states (event_id);

-- migrate:down

-- Remove event_id column from entity_states
ALTER TABLE entity_states DROP COLUMN IF EXISTS event_id;

-- Drop events schema tables (entity_states and entity_state_attributes remain)
DROP TABLE IF EXISTS events CASCADE;
DROP TABLE IF EXISTS event_rules CASCADE;
DROP TABLE IF EXISTS event_data CASCADE;
DROP TABLE IF EXISTS event_types CASCADE;
