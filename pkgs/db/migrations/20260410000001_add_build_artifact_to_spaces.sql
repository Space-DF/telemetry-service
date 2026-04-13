-- migrate:up
ALTER TABLE spaces
    ADD COLUMN IF NOT EXISTS build_artifact TEXT;

-- migrate:down
ALTER TABLE spaces
    DROP COLUMN IF EXISTS build_artifact;
