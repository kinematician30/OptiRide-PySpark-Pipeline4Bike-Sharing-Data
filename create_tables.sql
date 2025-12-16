-- Drop old tables if they exist (safe reset)
CREATE DATABASE bikeshare_dw;

-- Dimension: Station
CREATE TABLE dim_station (
    station_id VARCHAR PRIMARY KEY,
    station_name VARCHAR,
    latitude NUMERIC,
    longitude NUMERIC,
    address VARCHAR,
    has_ebikes BOOLEAN,
    slots INT
);

-- Dimension: Weather
CREATE TABLE dim_weather (
    weather_id BIGSERIAL PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    temperature NUMERIC,
    precipitation NUMERIC,
    wind_speed NUMERIC,
    cloud_cover NUMERIC,
    humidity NUMERIC
);

-- Fact Table: Bike Usage & Weather
CREATE TABLE fact_bike_weather (
    fact_id BIGSERIAL PRIMARY KEY,
    station_id VARCHAR REFERENCES dim_station(station_id),
      BIGINT REFERENCES dim_weather(weather_id),
    timestamp TIMESTAMP NOT NULL,
    free_bikes INT,
    empty_slots INT,
    total_slots INT,
    ebikes INT
);

-- Indexes for performance
CREATE INDEX idx_fact_bike_weather_station_id ON fact_bike_weather(station_id);
CREATE INDEX idx_fact_bike_weather_weather_id ON fact_bike_weather(weather_id);
CREATE INDEX idx_fact_bike_weather_timestamp ON fact_bike_weather(timestamp);
