-- migrate:up
ALTER TABLE automations ADD COLUMN IF NOT EXISTS is_deactivated BOOLEAN DEFAULT FALSE;
ALTER TABLE automations ADD COLUMN IF NOT EXISTS deactivated_at TIMESTAMP WITH TIME ZONE;

-- migrate:down

ALTER TABLE automations DROP COLUMN IF EXISTS is_deactivated;
ALTER TABLE automations DROP COLUMN IF EXISTS deactivated_at;
