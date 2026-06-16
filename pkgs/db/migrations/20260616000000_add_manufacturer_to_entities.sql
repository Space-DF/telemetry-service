-- migrate:up
CREATE EXTENSION IF NOT EXISTS pg_trgm;
ALTER TABLE entities ADD COLUMN IF NOT EXISTS manufacturer TEXT DEFAULT '';
CREATE INDEX IF NOT EXISTS idx_entities_manufacturer_trgm ON entities USING gin (manufacturer gin_trgm_ops);

-- migrate:down
DROP INDEX IF EXISTS idx_entities_manufacturer_trgm;
ALTER TABLE entities DROP COLUMN IF EXISTS manufacturer;