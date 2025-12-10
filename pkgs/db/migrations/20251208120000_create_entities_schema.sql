-- migrate:up

-- Create entities schema tables alongside existing device_locations table
-- Both are needed: device_locations for location history, entities for device state/attributes

CREATE TABLE IF NOT EXISTS entity_types (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    unique_key TEXT NOT NULL,
    image_url TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_entity_types_unique_key ON entity_types (unique_key);

CREATE TABLE IF NOT EXISTS entities (
    id UUID PRIMARY KEY,
    space_slug TEXT,
    device_id UUID NOT NULL,
    unique_key TEXT NOT NULL,
    category TEXT,
    entity_type_id UUID NOT NULL REFERENCES entity_types(id) ON DELETE CASCADE,
    name TEXT,
    unit_of_measurement TEXT,
    display_type VARCHAR(50) NOT NULL DEFAULT 'chart',
    image_url TEXT,
    is_enabled BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_entities_unique_key ON entities (unique_key);
CREATE INDEX IF NOT EXISTS idx_entities_space_slug ON entities (space_slug);
CREATE INDEX IF NOT EXISTS idx_entities_device_id ON entities (device_id);

CREATE TABLE IF NOT EXISTS entity_state_attributes (
    id UUID PRIMARY KEY,
    hash BIGINT NOT NULL,
    shared_attrs JSONB NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_entity_state_attributes_hash ON entity_state_attributes (hash);

CREATE TABLE IF NOT EXISTS entity_states (
    id UUID PRIMARY KEY,
    entity_id UUID NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
    state TEXT NOT NULL,
    attributes_id UUID REFERENCES entity_state_attributes(id),
    old_state_id UUID REFERENCES entity_states(id),
    reported_at TIMESTAMPTZ NOT NULL,
    last_changed_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_entity_states_entity_reported_at ON entity_states (entity_id, reported_at DESC);


-- migrate:down

-- Remove entities schema tables, leaving device_locations intact
DROP TABLE IF EXISTS entity_states CASCADE;
DROP TABLE IF EXISTS entity_state_attributes CASCADE;
DROP TABLE IF EXISTS entities CASCADE;
DROP TABLE IF EXISTS entity_types CASCADE;
