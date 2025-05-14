CREATE TABLE IF NOT EXISTS trips (
	route_id INT,
	service_id VARCHAR(50),
	trip_id VARCHAR(50),
	trip_headsign VARCHAR(50),
	direction_id BOOLEAN,
	shape_id VARCHAR(50),
	wheelchair_accessible SMALLINT
);

CREATE TABLE IF NOT EXISTS routes (
	route_id SMALLINT,
	agency_id SMALLINT,
	route_short_name VARCHAR(5),
	route_long_name VARCHAR(100),
	route_type SMALLINT,
	route_color VARCHAR(10),
	route_text_color VARCHAR(10)
);

CREATE TABLE IF NOT EXISTS shapes (
	shape_id VARCHAR(100),
	shape_pt_lat DOUBLE PRECISION,
	shape_pt_lon DOUBLE PRECISION,
	shape_pt_sequence SMALLINT
);

CREATE TABLE IF NOT EXISTS stop_times (
	trip_id VARCHAR(50),
	arrival_time TIME,
	departure_time TIME,
	stop_id SMALLINT,
	stop_sequence SMALLINT
);

CREATE TABLE IF NOT EXISTS stops (
	stop_id SMALLINT,
	stop_code VARCHAR(10),
	stop_name VARCHAR(100),
	stop_desc VARCHAR(100),
	stop_lat DOUBLE PRECISION,
	stop_lon DOUBLE PRECISION,
	zone_id VARCHAR(10),
	wheelchair_boarding SMALLINT
);

CREATE TABLE IF NOT EXISTS bus (
	bus_id SMALLINT,
	total_capacity INTEGER,
	seats SMALLINT
);

CREATE TABLE IF NOT EXISTS events (
	event_id SERIAL,
	event_name VARCHAR(120),
	day_event DATE,
	start_time TIME,
	end_time TIME,
	location_event VARCHAR(120) 
);

CREATE TABLE IF NOT EXISTS weather (
    measurement_id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    weather_code INTEGER,
    precipitation_probability DOUBLE PRECISION,
    temperature DOUBLE PRECISION,
    hour TIMESTAMP
);
-- CREATE TABLE bus_passenger_summary (
--     id SERIAL PRIMARY KEY,
--     stop_id TEXT NOT NULL,
--     route TEXT NOT NULL,
--     timestamp VARCHAR(120) NOT NULL,
--     trip_id TEXT NOT NULL,
--     passengers_off INTEGER DEFAULT 0,
--     passengers_on INTEGER DEFAULT 0,
--     UNIQUE (stop_id, route, timestamp, trip_id)
-- );

-- CREATE TABLE bus_passenger_summary (
--     stop_id TEXT,
--     route TEXT,
--     timestamp VARCHAR(120),
--     trip_id TEXT,
--     bus_id SMALLINT,
--     passengers_on INTEGER,
--     passengers_off INTEGER,
--     occupancy INTEGER,
--     occupancy_ratio DOUBLE PRECISION,
--     PRIMARY KEY (stop_id, route, timestamp, trip_id)
-- );


COPY trips (route_id,service_id,trip_id,trip_headsign,direction_id,shape_id,wheelchair_accessible)
FROM '/docker-entrypoint-initdb.d/trentino_trasporti/trips.txt'
-- FROM '/data/trentino_trasporti/trips.txt'
DELIMITER ','
CSV HEADER;

COPY routes (route_id,agency_id,route_short_name,route_long_name,route_type,route_color,route_text_color)
FROM '/docker-entrypoint-initdb.d/trentino_trasporti/routes.txt'
-- FROM '/data/trentino_trasporti/routes.txt'
DELIMITER ','
CSV HEADER;

COPY shapes (shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence)
FROM '/docker-entrypoint-initdb.d/trentino_trasporti/shapes.txt'
-- FROM '/data/trentino_trasporti/shapes.txt'
DELIMITER ','
CSV HEADER;

COPY stops (stop_id,stop_code,stop_name,stop_desc,stop_lat,stop_lon,zone_id,wheelchair_boarding)
FROM '/docker-entrypoint-initdb.d/trentino_trasporti/stops.txt'
-- FROM '/data/trentino_trasporti/stops.txt'
DELIMITER ','
CSV HEADER;

COPY stop_times (trip_id,arrival_time,departure_time,stop_id,stop_sequence)
FROM '/docker-entrypoint-initdb.d/trentino_trasporti/stop_times_cleaned.txt'
-- FROM '/data/trentino_trasporti/stop_times.txt'
DELIMITER ','
CSV HEADER;

COPY bus (bus_id,total_capacity,seats)
FROM '/docker-entrypoint-initdb.d/trentino_trasporti/buses.csv'
DELIMITER ','
CSV HEADER;

COPY events (event_id,event_name,day_event,start_time,end_time,location_event)
FROM '/docker-entrypoint-initdb.d/trentino_trasporti/events.csv'
DELIMITER ','
CSV HEADER;