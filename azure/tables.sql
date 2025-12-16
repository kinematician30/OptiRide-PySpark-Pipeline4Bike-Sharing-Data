-- 1. Dimension table for all networks (one-time load)
CREATE TABLE dim_network (
    id NVARCHAR(255) NOT NULL,
    name NVARCHAR(500),
    city NVARCHAR(255),
    country CHAR(2),
    latitude FLOAT,
    longitude FLOAT,
    href NVARCHAR(1000),
    gbfs_href NVARCHAR(1000)
);
ALTER TABLE dim_network ADD CONSTRAINT PK_dim_network PRIMARY KEY NONCLUSTERED (id) NOT ENFORCED;


-- 2. Dimension table for all stations
CREATE TABLE dim_station (
    station_id NVARCHAR(255) NOT NULL,
    network_id NVARCHAR(255), -- Foreign key to dim_network
    name NVARCHAR(500),
    latitude FLOAT,
    longitude FLOAT,
    uid NVARCHAR(100),         -- From 'extra'
    total_slots INT,           -- From 'extra.slots'
    payment_terminal BIT,
    virtual_station BIT
);
ALTER TABLE dim_station ADD CONSTRAINT PK_dim_station PRIMARY KEY NONCLUSTERED (station_id) NOT ENFORCED;


-- 3. Fact table for real-time station status
CREATE TABLE fact_station_status (
    status_id BIGINT IDENTITY(1,1), -- Auto-incrementing key
    station_id NVARCHAR(255) NOT NULL, -- Foreign key to dim_station
    timestamp DATETIME2,
    free_bikes INT,
    empty_slots INT,
    last_updated_epoch BIGINT
);
-- Optional: Create a clustered columnstore index for high-speed analytics
CREATE CLUSTERED COLUMNSTORE INDEX CCI_fact_station_status ON fact_station_status;
