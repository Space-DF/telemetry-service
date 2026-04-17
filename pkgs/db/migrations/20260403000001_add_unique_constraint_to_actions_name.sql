-- migrate:up
-- Add unique constraint on actions.name to prevent duplicate action names
ALTER TABLE actions
ADD CONSTRAINT actions_name_key UNIQUE (name);

-- migrate:down
-- Remove unique constraint on actions.name
ALTER TABLE actions
DROP CONSTRAINT IF EXISTS actions_name_key;