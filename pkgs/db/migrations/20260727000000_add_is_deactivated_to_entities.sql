-- migrate:up
ALTER TABLE entities ADD COLUMN IF NOT EXISTS is_deactivated BOOLEAN DEFAULT FALSE;

-- migrate:down
ALTER TABLE entities DROP COLUMN IF EXISTS is_deactivated;
