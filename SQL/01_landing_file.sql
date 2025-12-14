
-- MAIN GOALS
-- Copy files from central file server into the landing zone, saving the raw file
-- Register each file's metadata
-- Avoid to process the same file twice
-- Follow the status of each file (RECEIVED, PARSED, FAILED)


---------- creating the landing schame ----------
CREATE SCHEMA IF NOT EXISTS landing;

------------ creating the landing_file table ------------
CREATE TABLE landing.landing_file (
  file_id             BIGSERIAL PRIMARY KEY,                -- a serial number for file identification
  -- metadata from the source file
  source_system       TEXT NOT NULL,                        -- 'delivery_events' | 'complaints'
  source_file_path    TEXT NOT NULL,                        -- "\\fileserver\incoming\complaints\a.json"
  source_modified_at  TIMESTAMP NOT NULL,                   -- timestamp of modified at
  file_size_bytes     BIGINT NOT NULL,                      -- file size
  -- landing information
  landing_file_path   TEXT NOT NULL,                        -- "landing/complaints/ingest_dt=2025-12-13/a.json"
  -- control
  received_at         TIMESTAMP NOT NULL DEFAULT NOW(),     -- timestamp of file arrival
  parsed_at           TIMESTAMP,                            -- timestamp of file parsing. will be null when it arrives and modified later
  status              TEXT NOT NULL DEFAULT 'RECEIVED',     -- RECEIVED / PARSED / FAILED
  error_message       TEXT                                  -- error message from parsing logging
);

-- to guarantee uniqueness of landing files, we can use the file metadata and set an unique index
CREATE UNIQUE INDEX landing.ux_landing_file_natural
ON landing.landing_file (source_system, source_file_path, source_modified_at, file_size_bytes);

-- we can also create an index with status and received_at to make the parsing search quicker
CREATE INDEX landing.ix_landing_file_status_received
ON landing.landing_file (status, received_at);


------------ updating the landing_file table ------------
INSERT INTO landing.landing_file (
  source_system,
  source_file_path,
  source_modified_at,
  file_size_bytes,
  landing_file_path,
  status
)
VALUES (
  :source_system,
  :source_file_path,
  :source_modified_at,
  :file_size_bytes,
  :landing_file_path,
  'RECEIVED'
)
ON CONFLICT (source_system, source_file_path, source_modified_at, file_size_bytes)
DO NOTHING;
