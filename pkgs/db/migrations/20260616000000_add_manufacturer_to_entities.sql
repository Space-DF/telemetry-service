-- migrate:up
ALTER TABLE entities ADD COLUMN IF NOT EXISTS manufacturer TEXT DEFAULT '';
CREATE INDEX IF NOT EXISTS idx_entities_manufacturer ON entities (manufacturer);

-- migrate:down
DROP INDEX IF EXISTS idx_entities_manufacturer;
ALTER TABLE entities DROP COLUMN IF EXISTS manufacturer;
