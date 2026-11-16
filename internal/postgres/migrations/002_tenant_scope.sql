CREATE TABLE IF NOT EXISTS tenants (
    tenant_id BIGINT PRIMARY KEY CHECK (tenant_id > 0),
    tenant_name TEXT NOT NULL UNIQUE CHECK (length(tenant_name) BETWEEN 1 AND 128),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO tenants (tenant_id, tenant_name)
VALUES (1, 'legacy-local')
ON CONFLICT (tenant_id) DO NOTHING;

ALTER TABLE trip_state ADD COLUMN IF NOT EXISTS tenant_id BIGINT NOT NULL DEFAULT 1 REFERENCES tenants(tenant_id);
ALTER TABLE processed_telemetry ADD COLUMN IF NOT EXISTS tenant_id BIGINT NOT NULL DEFAULT 1 REFERENCES tenants(tenant_id);
ALTER TABLE aggregated_distance_events ADD COLUMN IF NOT EXISTS tenant_id BIGINT NOT NULL DEFAULT 1 REFERENCES tenants(tenant_id);

ALTER TABLE trip_state DROP CONSTRAINT IF EXISTS trip_state_pkey;
ALTER TABLE trip_state ALTER COLUMN tenant_id DROP DEFAULT;
ALTER TABLE trip_state ADD CONSTRAINT trip_state_pkey PRIMARY KEY (tenant_id, obu_id);

ALTER TABLE processed_telemetry DROP CONSTRAINT IF EXISTS processed_telemetry_pkey;
ALTER TABLE processed_telemetry ALTER COLUMN tenant_id DROP DEFAULT;
ALTER TABLE processed_telemetry ADD CONSTRAINT processed_telemetry_pkey PRIMARY KEY (tenant_id, event_id);

ALTER TABLE aggregated_distance_events DROP CONSTRAINT IF EXISTS aggregated_distance_events_pkey;
ALTER TABLE aggregated_distance_events ALTER COLUMN tenant_id DROP DEFAULT;
ALTER TABLE aggregated_distance_events ADD CONSTRAINT aggregated_distance_events_pkey PRIMARY KEY (tenant_id, event_id);

CREATE INDEX IF NOT EXISTS processed_telemetry_tenant_obu_idx ON processed_telemetry (tenant_id, obu_id);
CREATE INDEX IF NOT EXISTS aggregated_distance_events_tenant_obu_idx ON aggregated_distance_events (tenant_id, obu_id);
