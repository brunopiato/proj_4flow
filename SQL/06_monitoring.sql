
----- monitoring complaints -----
CREATE TABLE IF NOT EXISTS dw.monitoring_complaints (
    metric_date DATE PRIMARY KEY,
    complaints_loaded BIGINT NOT NULL,
    complaints_unresolved BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- updating the complaints monitoring table
INSERT INTO dw.monitoring_complaints (metric_date, complaints_loaded, complaints_unresolved)
SELECT
    CURRENT_DATE,
    COUNT(*) FILTER (WHERE created_at::date = CURRENT_DATE),
    COUNT(*) FILTER (WHERE pickup_station_id IS NULL AND created_at::date = CURRENT_DATE)
FROM dw.fact_complaint
ON CONFLICT (metric_date) DO UPDATE
SET
    complaints_loaded = EXCLUDED.complaints_loaded,
    complaints_unresolved = EXCLUDED.complaints_unresolved,
    updated_at = NOW();