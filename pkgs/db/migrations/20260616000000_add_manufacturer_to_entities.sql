-- migrate:up transaction:false
CREATE EXTENSION IF NOT EXISTS pg_trgm;
ALTER TABLE entities ADD COLUMN IF NOT EXISTS manufacturer TEXT DEFAULT '';
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_entities_manufacturer_trgm ON entities USING gin (manufacturer gin_trgm_ops);

-- migrate:down transaction:false
DROP INDEX CONCURRENTLY IF EXISTS idx_entities_manufacturer_trgm;
ALTER TABLE entities DROP COLUMN IF EXISTS manufacturer;
