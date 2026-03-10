-- migrate:up
ALTER TABLE geofences
ADD COLUMN IF NOT EXISTS features JSONB,
ADD COLUMN IF NOT EXISTS color TEXT;

-- migrate:down
ALTER TABLE geofences
DROP COLUMN IF EXISTS features,
DROP COLUMN IF EXISTS color;
