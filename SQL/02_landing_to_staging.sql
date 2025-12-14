-- MAIN GOALS:
-- Pegar os arquivos que já estão na landing layer (e estão marcados como RECEIVED na landing_file), ler o conteúdo (CSV/XML/JSON), transformar em linhas “padronizadas” e inserir nas tabelas stg_delivery_event e stg_complaint
-- E ao final marcar o arquivo como PARSED (ou FAILED) na landing_file


---------- iteratively scans the landing_file table ----------
SELECT file_id, source_system, landing_file_path    -- selects the columns that refer to the file
FROM landing.landing_file
WHERE status = 'RECEIVED'                           -- filtering only what has RECEIVED as status
ORDER BY received_at                                -- ordering by received_at
LIMIT 100;                                          -- limiting to 100 to avoid overload


---------- creating the staging schema ----------
CREATE SCHEMA IF NOT EXISTS staging;


----- complaints -----
CREATE TABLE staging.stg_complaint (
    -- file indentification
    file_id                    BIGINT NOT NULL REFERENCES landing_file(file_id),      -- reference to the landing_file.file_id
    record_index               INT NOT NULL,                                          -- entry position in the source file
    record_hash_sha256         TEXT NOT NULL UNIQUE,                                  -- a hash to guarantee entry uniqueness
    -- file content (data)
    complained_at              TIMESTAMP,                                             -- timestamp of complaint message
    pickup_station_external_id TEXT,                                                  -- pickup station identifier
    recipient_name             TEXT,                                                  -- final recipient name
    recipient_address          TEXT,                                                  -- final recipient address
    parcel_id                  TEXT,                                                  -- parcel identifier (optional)
    purchase_order_id          TEXT,                                                  -- PO identifier (optional)
    complaint_text             TEXT,                                                  -- complaint message
    -- normalized fields
    recipient_name_norm        TEXT,                                                  -- normalized final recipient name
    recipient_address_norm     TEXT,                                                  -- normalized final recipient address
    -- timestamp of parsing
    ingested_at                TIMESTAMP NOT NULL DEFAULT NOW(),
    -- defining the table's primary key
    PRIMARY KEY (file_id, record_index)
);



----- delivery_events -----
CREATE TABLE staging.stg_delivery_event (
    -- file indentification
    file_id                    BIGINT NOT NULL REFERENCES landing_file(file_id),      -- reference to the landing_file.file_id
    record_index               INT NOT NULL,                                          -- entry position in the source file
    record_hash_sha256         TEXT NOT NULL UNIQUE,                                  -- a hash to guarantee entry uniqueness
    -- file content (data)
    delivered_at               TIMESTAMP,                                             -- parcel deliery timestamp 
    pickup_station_external_id TEXT,                                                  -- pickup station identifier as defined by the company
    driver_id                  TEXT,                                                  -- driver identifier
    parcel_id                  TEXT,                                                  -- parcel identifier
    purchase_order_id          TEXT,                                                  -- PO identifier
    recipient_name             TEXT,                                                  -- final recipient name
    recipient_address          TEXT,                                                  -- final recipient address
    -- normalized fields
    recipient_name_norm        TEXT,                                                  -- normalized final recipient name
    recipient_address_norm     TEXT,                                                  -- normalized final recipient address
    -- timestamp of parsing
    ingested_at                TIMESTAMP NOT NULL DEFAULT NOW(),                      -- timestamp of parsing 
    -- defining the table's primary key
    PRIMARY KEY (file_id, record_index)                                               -- defines the combination of file_id and record_index as the table's primary key
);


----- updating landing_file if successes -----
UPDATE landing.landing_file
SET status = 'PARSED',
    parsed_at = NOW(),
    error_message = NULL
WHERE file_id = :file_id;


----- updating landing_file if file fails -----
UPDATE landing.landing_file
SET status = 'FAILED',
    error_message = :error_message
WHERE file_id = :file_id;
