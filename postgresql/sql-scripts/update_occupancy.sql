-- Create or update views for ticket and sensor counts
CREATE OR REPLACE VIEW ticket_counts AS
SELECT
    trip_id,
    bus_id,
    route,
    stop_id,
    timestamp,
    COUNT(*) AS passengers_on
FROM
    raw_tickets
GROUP BY
    trip_id, bus_id, route, stop_id, timestamp;

CREATE OR REPLACE VIEW sensor_counts AS
SELECT
    trip_id,
    bus_id,
    route,
    stop_id,
    timestamp,
    COUNT(*) AS passengers_off
FROM
    raw_sensors
WHERE
    status = 1  -- Assuming status 1 indicates people getting off
GROUP BY
    trip_id, bus_id, route, stop_id, timestamp;

-- Calculate current occupancy
CREATE OR REPLACE VIEW current_occupancy AS
SELECT
    t.bus_id,
    t.trip_id,
    MAX(t.timestamp) AS latest_timestamp,
    GREATEST(SUM(COALESCE(t.passengers_on, 0) - COALESCE(s.passengers_off, 0)), 0) AS total_occupancy
FROM
    ticket_counts t
LEFT JOIN
    sensor_counts s
ON
    t.trip_id = s.trip_id AND
    t.bus_id = s.bus_id AND
    t.route = s.route AND
    t.stop_id = s.stop_id AND
    t.timestamp = s.timestamp
GROUP BY
    t.bus_id, t.trip_id;

-- Join with bus table and calculate occupancy percentage
CREATE OR REPLACE VIEW bus_occupancy AS
SELECT
    co.bus_id,
    co.trip_id,
    co.latest_timestamp,
    co.total_occupancy,
    b.total_capacity,
    (co.total_occupancy::float / b.total_capacity) * 100 AS occupancy_percentage
FROM
    current_occupancy co
JOIN
    bus b ON co.bus_id = b.bus_id;

-- Update total occupancy
INSERT INTO total_occupancy (trip_id, bus_id, route, stop_id, total_passengers)
SELECT
    trip_id,
    bus_id,
    route,
    stop_id,
    total_occupancy
FROM
    current_occupancy
ON CONFLICT (trip_id, bus_id, route, stop_id)
DO UPDATE SET
    total_passengers = total_occupancy.total_passengers + EXCLUDED.total_passengers;