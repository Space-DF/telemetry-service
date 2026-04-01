-- migrate:up
-- Add title field to automations table
ALTER TABLE automations
ADD COLUMN IF NOT EXISTS title VARCHAR(255);

-- Remove entity_id from events table as it's no longer needed
ALTER TABLE events
DROP COLUMN IF EXISTS entity_id;


-- migrate:down
ALTER TABLE automations
DROP COLUMN IF EXISTS title;

ALTER TABLE events
ADD COLUMN IF NOT EXISTS entity_id TEXT;
