-- O que esse passo precisa resolver (em termos simples)
-- 1. Copiar os arquivos do file server para um local controlado (Landing)
-- 2. Guardar o arquivo original, sem alterar nada
-- 3. Registrar metadados sobre cada arquivo
-- 4. Evitar processar o mesmo arquivo duas vezes
-- 5. Saber o status de cada arquivo (novo, processado, erro)

---------- creating the landing schame ----------
CREATE SCHEMA IF NOT EXISTS landing;

------------ creating the landing_file table ------------
CREATE TABLE landing.landing_file (
  file_id             BIGSERIAL PRIMARY KEY,                -- a serial number for file identification
  -- source identification
  source_system       TEXT NOT NULL,                        -- 'delivery_events' | 'complaints'
  -- metadata from the source file
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
