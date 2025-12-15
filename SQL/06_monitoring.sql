
----- monitoring complaints -----
CREATE TABLE IF NOT EXISTS dw.monitoring_complaints (
    monitoring_date DATE PRIMARY KEY,                       -- date of the monitoring process. runs daily
    complaints_loaded BIGINT NOT NULL,                      -- number of rows (complaints) processed in the current day
    complaints_unresolved BIGINT NOT NULL,                  -- number of rows (complaints) processed in the current day that didnt get a pickup_station_id from dim_pickup_station
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()           -- timestamp of update of the table
);

-- updating the complaints monitoring table
INSERT INTO dw.monitoring_complaints (monitoring_date, complaints_loaded, complaints_unresolved)
SELECT
    CURRENT_DATE,                                                                           -- date of the monitoring event
    COUNT(*) FILTER (WHERE created_at::date = CURRENT_DATE),                                -- counting of rows (complaints) created in the same day
    COUNT(*) FILTER (WHERE pickup_station_id IS NULL AND created_at::date = CURRENT_DATE)   -- counting of rows (complaints) that didn't get a pickup_station_id from dimensional table
FROM dw.fact_complaint
ON CONFLICT (monitoring_date) DO UPDATE                                                     -- on conflict of monitoring date updates the table
SET
    complaints_loaded = EXCLUDED.complaints_loaded,                                         -- sticks with the latest counting of rows (complaints)
    complaints_unresolved = EXCLUDED.complaints_unresolved,                                 -- sticks with the latest counting of rows (complaints) without pickup_station_id
    updated_at = NOW();                                                                     -- updates timestamp of processing