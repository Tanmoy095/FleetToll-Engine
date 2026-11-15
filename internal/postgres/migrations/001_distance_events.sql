CREATE TABLE IF NOT EXISTS trip_state (
    obu_id BIGINT PRIMARY KEY CHECK (obu_id > 0),
    latitude DOUBLE PRECISION NOT NULL CHECK (latitude BETWEEN -90 AND 90),
    longitude DOUBLE PRECISION NOT NULL CHECK (longitude BETWEEN -180 AND 180),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS processed_telemetry (
    event_id TEXT PRIMARY KEY,
    obu_id BIGINT NOT NULL CHECK (obu_id > 0),
    distance_km DOUBLE PRECISION NOT NULL CHECK (distance_km >= 0),
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS aggregated_distance_events (
    event_id TEXT PRIMARY KEY,
    obu_id BIGINT NOT NULL CHECK (obu_id > 0),
    distance_km DOUBLE PRECISION NOT NULL CHECK (distance_km >= 0),
    event_unix BIGINT NOT NULL CHECK (event_unix > 0),
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS aggregated_distance_events_obu_idx
    ON aggregated_distance_events (obu_id);
