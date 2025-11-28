-- migrate:up
ALTER TABLE device_locations RENAME COLUMN organization_slug TO space_slug;

-- migrate:down
ALTER TABLE device_locations RENAME COLUMN space_slug TO organization_slug;
