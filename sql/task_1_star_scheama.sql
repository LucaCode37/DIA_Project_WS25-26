--Star schema for DB DATA 
-- 4 Dimension tables and 1 Fact table


--First Dim station
CREATE TABLE dim_station (
    station_id      BIGSERIAL PRIMARY KEY,
    eva             BIGINT NOT NULL,
    station_name    TEXT NOT NULL,
    latitude        DOUBLE PRECISION,
    longitude       DOUBLE PRECISION,
    federal_state   TEXT,
    UNIQUE (eva)
);


--Second Dim Tiem
CREATE TABLE dim_time (
    time_id     BIGSERIAL PRIMARY KEY,
    db_time_str TEXT NOT NULL UNIQUE,
    ts          TIMESTAMPTZ NOT NULL,
    year        INT NOT NULL,
    month       INT NOT NULL,
    day         INT NOT NULL,
    hour        INT NOT NULL,
    minute      INT NOT NULL,
    dow         INT NOT NULL
);


--Third Dim Snapshot
CREATE TABLE dim_snapshot (
    snapshot_id     BIGSERIAL PRIMARY KEY,
    snapshot_str   TEXT NOT NULL UNIQUE,
    snapshot_ts    TIMESTAMPTZ NOT NULL,
    granularity    TEXT NOT NULL CHECK (granularity IN ('HOUR', 'MIN15'))
);


--Fourth Dim Train
CREATE TABLE dim_train ( 
    train_id        BIGSERIAL PRIMARY KEY,
    operator_code   TEXT,
    category        TEXT,
    train_number    TEXT,
    train_type      TEXT,
    train_flag      TEXT,
    UNIQUE NULLS NOT DISTINCT (operator_code, category, train_number, train_type, train_flag)
);


--Fact Table Train Movement
CREATE TABLE fact_train_movement (
    movement_id     BIGSERIAL PRIMARY KEY,

    station_id      BIGINT NOT NULL REFERENCES dim_station(station_id),
    train_id        BIGINT REFERENCES dim_train(train_id),

    snapshot_id    BIGINT NOT NULL REFERENCES dim_snapshot(snapshot_id),

    stop_natural_id TEXT NOT NULL,

    planned_arrival_time_id     BIGINT REFERENCES dim_time(time_id),
    planned_departure_time_id   BIGINT REFERENCES dim_time(time_id),
    
    current_arrival_time_id     BIGINT REFERENCES dim_time(time_id),
    current_departure_time_id   BIGINT REFERENCES dim_time(time_id),

    platform_planned TEXT,
    platform_current TEXT,
    line_code        TEXT,

    arrival_delay_min    INT,
    departure_delay_min  INT,
    is_cancelled         BOOLEAN DEFAULT FALSE,

    arrival_path    TEXT,
    departure_path  TEXT,


    UNIQUE (snapshot_id, stop_natural_id, station_id)
);


CREATE INDEX idx_dim_snapshot_ts ON dim_snapshot(snapshot_ts);
CREATE INDEX idx_dim_station_name ON dim_station(station_name);
CREATE INDEX idx_fact_station_movement_station ON fact_train_movement(station_id);
CREATE INDEX idx_fact_station_movement_train ON fact_train_movement(train_id);
CREATE INDEX idx_fact_station_movement_snapshot ON fact_train_movement(snapshot_id);
CREATE INDEX idx_fact_station_movement_cancelled ON fact_train_movement(is_cancelled) WHERE is_cancelled = TRUE;