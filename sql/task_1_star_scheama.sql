CREATE TABLE dim_station (
    station_id      BIGSERIAL PRIMARY KEY,
    eva             BIGINT,
    station_name    TEXT NOT NULL,
    latitude        DOUBLE PRECISION,
    longitude       DOUBLE PRECISION,
    federal_state   TEXT,
    UNIQUE (eva)
);

CREATE TABLE dim_time (
    time_id     BIGSERIAL PRIMARY KEY,
    db_time_str TEXT NOT NULL UNIQUE,
    ts          TIMESTAMP NOT NULL,
    year        INT NOT NULL,
    month       INT NOT NULL,
    day         INT NOT NULL,
    hour        INT NOT NULL,
    minute      INT NOT NULL,
    dow         INT NOT NULL
);

CREATE TABLE dim_snapshot (
    snapshot_id     BIGSERIAL PRIMARY KEY,
    snapshot_str   TEXT NOT NULL UNIQUE,
    snapshot_ts    TIMESTAMP NOT NULL,
    granularity    TEXT NOT NULL CHECK (granularity IN ('HOUR', 'MIN15'))
);

CREATE TABLE dim_train ( 
    train_id        BIGSERIAL PRIMARY KEY,
    operator_code   TEXT,
    category        TEXT,
    train_number    TEXT,
    train_type      TEXT,
    train_flag      TEXT,
    UNIQUE (operator_code, category, train_number, train_type, train_flag)
);

CREATE TABLE fact_train_movement (
    movement_id     BIGSERIAL PRIMARY KEY,

    station_id      BIGINT NOT NULL REFERENCES dim_station(station_id),
    train_id        BIGINT NOT NULL REFERENCES dim_train(train_id),

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

CREATE INDEX idx_fact_station_movement_station ON fact_train_movement(station_id)
CREATE INDEX idx_fact_station_movement_train ON fact_train_movement(train_id)
CREATE INDEX idx_fact_station_movement_snapshot ON fact_train_movement(snapshot_id)
CREATE INDEX idx_fact_station_movement_planned_arrival ON fact_train_movement(planned_arrival_time_id)
CREATE INDEX idx_fact_station_movement_planned_departure ON fact_train_movement(planned_departure_time_id)
CREATE INDEX idx_fact_station_movement_current_arrival ON fact_train_movement(current_arrival_time_id)
CREATE INDEX idx_fact_station_movement_current_departure ON fact_train_movement(current_departure_time_id)