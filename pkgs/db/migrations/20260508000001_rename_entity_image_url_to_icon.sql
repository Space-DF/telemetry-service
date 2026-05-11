-- migrate:up
ALTER TABLE entities RENAME COLUMN image_url TO icon;

-- migrate:down
ALTER TABLE entities RENAME COLUMN icon TO image_url;