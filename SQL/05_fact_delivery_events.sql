---------- fact_delivery_event ----------
CREATE TABLE IF NOT EXISTS dw.fact_delivery_event (
    -- primary key
    delivery_event_sk         BIGSERIAL PRIMARY KEY,                                        -- surrogate key for the entry
    -- tracking of staging table
    record_hash_sha256        TEXT NOT NULL UNIQUE,                                         -- a hash to guarantee entry uniqueness
    file_id                   BIGINT NOT NULL REFERENCES landing.landing_file(file_id),     -- reference to the landing_file.file_id
    -- delivery event data
    delivered_at              TIMESTAMPTZ,                                                  -- 
    pickup_station_external_id TEXT,                                                        
    driver_id                 TEXT,                                                         -- identifier of the driver as in the source file
    parcel_id                 TEXT,                                                         -- identifier of the parcel as in the source file
    purchase_order_id         TEXT,                                                         -- identifier of the PO as in the file
    recipient_name            TEXT,                                                         -- final recipient name as in the source file
    recipient_address         TEXT,                                                         -- final recipient address as in the source file
    recipient_name_norm       TEXT,                                                         -- normalized final recipient name as in the source file
    recipient_address_norm    TEXT,                                                         -- normalized final recipient address as in the source file
    -- timing
    created_at                TIMESTAMPTZ NOT NULL DEFAULT NOW()                            -- timestamp of data insertion
);

-- speed up queries requiring delivery timestamp
CREATE INDEX IF NOT EXISTS dw_ix_fact_delivery_event_delivered_at
ON dw.fact_delivery_event(delivered_at);



----- DELIVERY EVENTS DATA INSERTION -----
INSERT INTO dw.fact_delivery_event (
  record_hash_sha256,
  file_id,
  delivered_at,
  pickup_station_external_id,
  driver_id,
  parcel_id,
  purchase_order_id,
  recipient_name,
  recipient_address,
  recipient_name_norm,
  recipient_address_norm
)
SELECT
  s.record_hash_sha256,
  s.file_id,
  s.delivered_at,
  s.pickup_station_external_id,
  s.driver_id,
  s.parcel_id,
  s.purchase_order_id,
  s.recipient_name,
  s.recipient_address,
  s.recipient_name_norm,
  s.recipient_address_norm
FROM staging.stg_delivery_event s
ON CONFLICT (record_hash_sha256) DO NOTHING;
