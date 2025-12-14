
---------- fact_complaints ----------
CREATE TABLE IF NOT EXISTS dw.fact_complaint (
    -- primary key
    complaint_sk              BIGSERIAL PRIMARY KEY,                                        -- surrogate key for the entry
    -- tracking of staging table
    record_hash_sha256        TEXT NOT NULL UNIQUE,                                         -- a hash to guarantee entry uniqueness
    file_id                   BIGINT NOT NULL REFERENCES landing.landing_file(file_id),     -- reference to the landing_file.file_id
    -- complaint data
    complained_at             TIMESTAMPTZ,                                                  -- timestamp of complaint message
    pickup_station_id         BIGINT,                                                       -- canonical ID
    pickup_station_external_id TEXT,                                                        -- pickup station ID as informed by the source file
    recipient_name            TEXT,                                                         -- final recipient name as informed by the source file
    recipient_address         TEXT,                                                         -- final recipient address as informed by the source file
    recipient_name_norm       TEXT,                                                         -- normalized final recipient name as informed by the source file
    recipient_address_norm    TEXT,                                                         -- normalized final recipient address as informed by the source file
    complaint_text            TEXT,                                                         -- complaint text as informed by the source file
    -- optional identifiers
    purchase_order_id         TEXT,                                                         -- (optional) PO ID as informed by the source file
    parcel_id                 TEXT,                                                         -- (optional) parcel ID as informed by the source file
    -- timing 
    created_at                TIMESTAMPTZ NOT NULL DEFAULT NOW()                            -- timestamp of insertion
);

-- speed up queries requiring complaint timestamp
CREATE INDEX IF NOT EXISTS dw_ix_fact_complaint_complained_at
ON dw.fact_complaint(complained_at);

-- speed up queries requiring pickup_station_id
CREATE INDEX IF NOT EXISTS dw_ix_fact_complaint_pickup_station_id
ON dw.fact_complaint(pickup_station_id);



----- COMPLAINTS DATA INSERTION -----
INSERT INTO dw.fact_complaint (
  record_hash_sha256,
  file_id,
  complained_at,
  pickup_station_external_id,
  recipient_name,
  recipient_address,
  recipient_name_norm,
  recipient_address_norm,
  purchase_order_id,
  parcel_id,
  complaint_text
)
SELECT
  s.record_hash_sha256,
  s.file_id,
  s.complained_at,
  s.pickup_station_external_id,
  s.recipient_name,
  s.recipient_address,
  s.recipient_name_norm,
  s.recipient_address_norm,
  s.purchase_order_id,
  s.parcel_id,
  s.complaint_text
FROM staging.stg_complaint s
ON CONFLICT (record_hash_sha256) DO NOTHING;

-- updating pickup_station_id
UPDATE dw.fact_complaint f
SET pickup_station_id = d.pickup_station_id
FROM dw.dim_pickup_station d
WHERE f.pickup_station_id IS NULL
  AND d.is_current = TRUE
  AND f.pickup_station_external_id = d.ref_identifier;
