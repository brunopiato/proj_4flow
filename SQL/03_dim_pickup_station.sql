
-- MAIN GOALS
-- Get a daily snapshot of the reference table and persist it in the staging zone
-- Compare the snapshot with the actual dim_pickup_stations version
-- Add new stations
-- Update existing stations (close old entry and insert new entry)
-- Deactivate removes stations
-- We will always use the identifier in the reference table as main key. 
-- If it does not work, we will use a fingerprint composed by station zip code and street


---------- staging.stg_pickup_station ----------
-- creates an intermediate table to receive the data from the reference table, creating control columns for timing and fingerprinting
CREATE TABLE IF NOT EXISTS staging.stg_pickup_station (
    -- table control
    snapshot_date      DATE NOT NULL,                 -- snapshot date
    -- reference table data
    ref_identifier     TEXT NOT NULL,                 -- identifier of the station as provided by the reference table
    name               TEXT,                          -- name of the station as provided by the reference table
    city               TEXT,                          -- city of the station as provided by the reference table
    zip_code           TEXT,                          -- zip code of the station as provided by the reference table
    street             TEXT,                          -- street of the station as provided by the reference table
    -- columns for change detection
    fingerprint        TEXT NOT NULL,                 -- a fingerprint with normalized zip_code and street
    attr_hash_sha256   TEXT NOT NULL,                 -- a hash to test if any field has changed
    -- key_definition
    PRIMARY KEY (snapshot_date, ref_identifier)       -- primary key definition
);

-- loading data into staging.stg_pickup_station
INSERT INTO staging.stg_pickup_station (
    snapshot_date,
    ref_identifier,
    name,
    city,
    zip_code,
    street,
    fingerprint,
    attr_hash_sha256
)
SELECT
    -- control columns
    CURRENT_DATE,                                               -- date of the snapshot
    -- referecen data columns 
    identifier,                                                 -- identifier of the station as provided by the reference table
    name,                                                       -- name of the station as provided by the reference table
    city,                                                       -- city of the station as provided by the reference table
    zip_code,                                                   -- zip code of the station as provided by the reference table
    street,                                                     -- street of the station as provided by the reference table
    -- change detection columns
    upper(
        coalesce(zip_code,'') || '|' || coalesce(street,'')
        ) AS fingerprint,                                       -- fingerprint with upper zip_code and street
    encode(
        digest(
            coalesce(name,'') || '|' ||
            coalesce(city,'') || '|' ||
            coalesce(zip_code,'') || '|' ||
            coalesce(street,''), 
            'sha256'),
        'hex') AS attr_hash_sha256                              -- creates hash considering the describing columns
FROM ref.pickup_locations
ON CONFLICT (snapshot_date, ref_identifier) DO UPDATE           -- if it runs more than once a day, updates table with:
SET
    name = EXCLUDED.name,                                       -- new entry of the name of the station 
    city = EXCLUDED.city,                                       -- new entry of the city of the station 
    zip_code = EXCLUDED.zip_code,                               -- new entry of the zip_code of the station 
    street = EXCLUDED.street,                                   -- new entry of the street of the station 
    fingerprint = EXCLUDED.fingerprint,                         -- new entry of the fingerprit of the station 
    attr_hash_sha256 = EXCLUDED.attr_hash_sha256;               -- new entry of the hash of the station 



---------- creating the dw schema ----------
CREATE SCHEMA IF NOT EXISTS dw;


---------- dim_pickup_station ----------
-- creates a sequence to be used in the pickup_station_id generation
CREATE SEQUENCE IF NOT EXISTS dw.pickup_station_id_seq;

-- loads PostgreSQL extension for hashing
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- creates the dim_pickup_station table
CREATE TABLE IF NOT EXISTS dw.dim_pickup_station (
    -- identification columns and keys
    pickup_station_sk   BIGSERIAL PRIMARY KEY,          -- surrogate key for the dimension table. defines the table row (version)
    pickup_station_id   BIGINT NOT NULL,                -- canonical ID for the station pickup. defines the pickup station
    -- reference data
    ref_identifier      TEXT NOT NULL,                  -- identifier of the station as provided by the reference table
    name                TEXT,                           -- name of the station as provided by the reference table
    city                TEXT,                           -- city of the station as provided by the reference table
    zip_code            TEXT,                           -- zip code of the station as provided by the reference table
    street              TEXT,                           -- street of the station as provided by the reference table
    -- columns for change detection
    fingerprint        TEXT NOT NULL,                   -- normalized zip_code and street
    attr_hash_sha256    TEXT NOT NULL                   -- a fingerprint to test if any field has changed
    -- SCD2 columns
    valid_from          TIMESTAMPTZ NOT NULL,           -- the timestamp from what the record is valid
    valid_to            TIMESTAMPTZ,                    -- the timestamp until what the record was valid
    is_current          BOOLEAN NOT NULL DEFAULT TRUE,  -- whether the record is the current version of the station
    is_active           BOOLEAN NOT NULL DEFAULT TRUE,  -- whether the record is active or inactive (if it was removed from the source)
);

-- guarantees that every pickup_station_id has only one current version
CREATE UNIQUE INDEX IF NOT EXISTS dw_ux_dim_pickup_station_current
ON dw.dim_pickup_station(pickup_station_id)
WHERE is_current = TRUE;

-- speed up matching by ref_identifier
CREATE INDEX IF NOT EXISTS dw_ix_dim_pickup_station_ref_current
ON dw.dim_pickup_station(ref_identifier)
WHERE is_current = TRUE;

-- speed up matching by fingerprint
CREATE INDEX IF NOT EXISTS dw_ix_dim_pickup_station_fingerprint_current
ON dw.dim_pickup_station(fingerprint)
WHERE is_current = TRUE;


-- first load into dw.dim_pickup_station
INSERT INTO dw.dim_pickup_station (
    pickup_station_id,
    ref_identifier,
    name,
    city,
    zip_code,
    street,
    fingerprint,
    attr_hash_sha256,
    valid_from, valid_to,
    is_current,
    is_active
)
SELECT
    -- identification column and key
    nextval('dw.pickup_station_id_seq') AS pickup_station_id,   -- defining the pickup_station_id canonical ID
    -- reference table data
    s.ref_identifier,                                           -- identifier of the station as provided by the reference table 
    s.name,                                                     -- name of the station as provided by the reference table
    s.city,                                                     -- city of the station as provided by the reference table
    s.zip_code,                                                 -- zip code of the station as provided by the reference table
    s.street,                                                   -- street of the station as provided by the reference table
    -- change detection columns
    s.fingerprint,                                              -- normalized zip_code and street
    s.attr_hash_sha256,                                         -- a fingerprint to test if any field has changed
    -- SCD2
    NOW() AS valid_from,                                        -- defining the timestamp from when the record is valid
    NULL::timestamptz AS valid_to,                              -- defining as NULL the timestamp until when the record is valid
    TRUE AS is_current,                                         -- defining as the current record
    TRUE AS is_active                                           -- defining as an active record
FROM staging.stg_pickup_station s
WHERE s.snapshot_date = CURRENT_DATE;


---------- daily loads ----------
----- 01.ADD -----
-- first we need to add all the new pickup stations in the reference table
INSERT INTO dw.dim_pickup_station (
  pickup_station_id,
  ref_identifier,
  name,
  city,
  zip_code,
  street,
  fingerprint,
  attr_hash_sha256,
  valid_from, valid_to,
  is_current,
  is_active
)
SELECT
    -- identification column and key
    nextval('dw.pickup_station_id_seq') AS pickup_station_id,       -- defining the pickup_station_id canonical ID
    -- reference table data
    s.ref_identifier,                                               -- identifier of the station as provided by the reference table 
    s.name,                                                         -- name of the station as provided by the reference table
    s.city,                                                         -- city of the station as provided by the reference table
    s.zip_code,                                                     -- zip code of the station as provided by the reference table
    s.street,                                                       -- street of the station as provided by the reference table
    -- change detection columns
    s.fingerprint,                                                  -- normalized zip_code and street
    s.attr_hash_sha256,                                             -- a fingerprint to test if any field has changed
    -- SCD2
    NOW() AS valid_from,                                            -- defining the timestamp from when the record is valid
    NULL::timestamptz AS valid_to,                                  -- defining as NULL the timestamp until when the record is valid
    TRUE AS is_current,                                             -- defining as the current record
    TRUE AS is_active                                               -- defining as an active record
FROM dw.stg_pickup_station s
LEFT JOIN dw.dim_pickup_station d_ref
    ON d_ref.ref_identifier = s.ref_identifier                      -- joins preferably by ref_identifier
    AND d_ref.is_current = TRUE
LEFT JOIN dw.dim_pickup_station d_fp
    ON d_fp.fingerprint = s.fingerprint                             -- after that joins by fingerprint
    AND d_fp.is_current = TRUE
WHERE s.snapshot_date = CURRENT_DATE                                -- filters by current date
    AND d_ref.pickup_station_sk IS NULL                             -- filters by null pickup_station_sk (new station) in first join
    AND d_fp.pickup_station_sk IS NULL;                             -- filter by pickup_station_sk (new station) in second join


----- 02.UPDATE CLOSE -----
-- after adding all the new stations, we need to identify the stations 
-- that changed and close the old entries so we can update the information.
WITH today AS (
    SELECT *
    FROM dw.stg_pickup_station
    WHERE snapshot_date = CURRENT_DATE
),
-- the "resolved" table connects the reference table in "today" table
-- with the dim_pickup_station table to get what station are related to what.
resolved AS (
    SELECT
        t.*,
        COALESCE(d_ref.pickup_station_id, d_fp.pickup_station_id) AS pickup_station_id  -- if the id was not found in the first join (ref_identifier), it uses the second join (fingerprint)
    FROM today t
    LEFT JOIN dw.dim_pickup_station d_ref
        ON d_ref.ref_identifier = t.ref_identifier
        AND d_ref.is_current = TRUE
    LEFT JOIN dw.dim_pickup_station d_fp
        ON d_fp.fingerprint = t.fingerprint
        AND d_fp.is_current = TRUE
),
-- the "to_close" table identifies what pickup stations should be 
-- closed so they can be changed in the next step.
to_close AS (
    SELECT d.pickup_station_sk
    FROM dw.dim_pickup_station d
    JOIN resolved r
    ON r.pickup_station_id = d.pickup_station_id
        WHERE d.is_current = TRUE
        AND (
            d.attr_hash_sha256 <> r.attr_hash_sha256
            OR d.ref_identifier <> r.ref_identifier
        )
)
-- updates the pickup station to be closed
UPDATE dw.dim_pickup_station d
SET valid_to = NOW(),
    is_current = FALSE
WHERE d.pickup_station_sk IN (SELECT pickup_station_sk FROM to_close);


----- 03.UPDATE INSERT -----
-- after we close the obsolete entries, we need to insert the
-- actual data for the pickup stations that were changed
WITH today AS (
    SELECT *
    FROM dw.stg_pickup_station
    WHERE snapshot_date = CURRENT_DATE
),
-- the "resolved" table connects the reference table in "today" table
-- with the dim_pickup_station table to get what station are related to what.
resolved AS (
    SELECT
        t.ref_identifier,
        t.name,
        t.city,
        t.zip_code,
        t.street,
        t.fingerprint,
        t.attr_hash_sha256,
        COALESCE(d_ref.pickup_station_id, d_fp.pickup_station_id) AS pickup_station_id  -- if the id was not found in the first join (ref_identifier), it uses the second join (fingerprint)
    FROM today t
    LEFT JOIN dw.dim_pickup_station d_ref
        ON d_ref.ref_identifier = t.ref_identifier
        AND d_ref.is_current = TRUE
    LEFT JOIN dw.dim_pickup_station d_fp
        ON d_fp.fingerprint = t.fingerprint
        AND d_fp.is_current = TRUE
),
-- the "need_insertion" table identifies what pickup stations were changed 
-- and need to be inserted after being closed in the previous step.
need_insertion AS (
    SELECT r.*
    FROM resolved r
    LEFT JOIN dw.dim_pickup_station d_cur
        ON d_cur.pickup_station_id = r.pickup_station_id
        AND d_cur.is_current = TRUE
    WHERE r.pickup_station_id IS NOT NULL
        AND d_cur.pickup_station_sk IS NULL
)
-- inserts the information about the changed pickup stations
INSERT INTO dw.dim_pickup_station (
    pickup_station_id,
    ref_identifier,
    name,
    city,
    zip_code,
    street,
    fingerprint,
    attr_hash_sha256,
    valid_from, valid_to,
    is_current,
    is_active
)
SELECT
    pickup_station_id,
    ref_identifier,
    name,
    city,
    zip_code,
    street,
    fingerprint,
    attr_hash_sha256,
    NOW() AS valid_from,
    NULL::timestamptz AS valid_to,
    TRUE AS is_current,
    TRUE AS is_active
FROM need_insert;


----- 04.REMOVE -----
-- finally we need to deactivate all the stations that were 
-- removed from the reference table
WITH today AS (
    SELECT *
    FROM dw.stg_pickup_station
    WHERE snapshot_date = CURRENT_DATE
),
-- the "resolved" table connects the reference table in "today" table
-- with the dim_pickup_station table to get what station are related to what.
resolved AS (
    SELECT
        COALESCE(d_ref.pickup_station_id, d_fp.pickup_station_id) AS pickup_station_id  -- if the id was not found in the first join (ref_identifier), it uses the second join (fingerprint)
    FROM today t
    LEFT JOIN dw.dim_pickup_station d_ref
        ON d_ref.ref_identifier = t.ref_identifier
        AND d_ref.is_current = TRUE
    LEFT JOIN dw.dim_pickup_station d_fp
        ON d_fp.fingerprint = t.fingerprint
        AND d_fp.is_current = TRUE
),
-- the "today_ids" identifies which are the current pickup stations
today_ids AS (
    SELECT DISTINCT pickup_station_id
    FROM resolved
    WHERE pickup_station_id IS NOT NULL
)
-- deactivates all the stations outside the "today_ids"
UPDATE dw.dim_pickup_station d
SET valid_to = NOW(),
    is_current = FALSE,
    is_active = FALSE
WHERE d.is_current = TRUE
    AND NOT EXISTS (
        SELECT 1
        FROM today_ids t
        WHERE t.pickup_station_id = d.pickup_station_id
    );
