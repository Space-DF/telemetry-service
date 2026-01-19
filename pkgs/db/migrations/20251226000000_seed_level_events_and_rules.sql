-- migrate:up
-- Seed predefined level events and event rules for common device types

-- Battery low level event (for any device with battery)
INSERT INTO level_events (owner_type, description, event_type_id)
SELECT
    'system',
    'Battery level is below threshold',
    event_type_id
FROM event_types
WHERE event_type = 'device_event'
ON CONFLICT DO NOTHING;

-- Get the battery_low level_event_id for creating rules (this would be done in application code usually)
-- For now, we'll create device_event level events for common scenarios

-- GPS position update event
INSERT INTO level_events (owner_type, description, event_type_id)
SELECT
    'system',
    'GPS position updated',
    event_type_id
FROM event_types
WHERE event_type = 'device_event'
ON CONFLICT DO NOTHING;

-- Temperature threshold event
INSERT INTO level_events (owner_type, description, event_type_id)
SELECT
    'system',
    'Temperature threshold exceeded',
    event_type_id
FROM event_types
WHERE event_type = 'device_event'
ON CONFLICT DO NOTHING;

-- Humidity threshold event
INSERT INTO level_events (owner_type, description, event_type_id)
SELECT
    'system',
    'Humidity threshold exceeded',
    event_type_id
FROM event_types
WHERE event_type = 'device_event'
ON CONFLICT DO NOTHING;

-- Motion detected event
INSERT INTO level_events (owner_type, description, event_type_id)
SELECT
    'system',
    'Motion detected',
    event_type_id
FROM event_types
WHERE event_type = 'device_event'
ON CONFLICT DO NOTHING;

-- Note: Event rules with entity_id and device_model_id require UUIDs from device-service
-- These would typically be created through the API or in application code
-- Example rule patterns (to be created via API):
--
-- Battery low for RAK4630:
--   level_event_id: <battery_low_level_event_id>
--   device_model_id: <rak4630_model_uuid>
--   rule_key: 'battery_low'
--   operator: 'lt'
--   operand: '20'
--   entity_id: would reference the battery entity for the device
--
-- High temperature for RAK4630:
--   level_event_id: <temp_high_level_event_id>
--   device_model_id: <rak4630_model_uuid>
--   rule_key: 'temperature_high'
--   operator: 'gt'
--   operand: '40'

-- migrate:down

-- Remove seeded level events
DELETE FROM level_events WHERE owner_type = 'system';
