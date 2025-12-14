
---------- REPORT 1 ----------
-- Identifier, Name and Zip Code of Pickup locations affected 
-- by claims together with the according number of claims within 
-- the last 14 calendar days
CREATE OR REPLACE VIEW dw.rpt_pickup_station_claims_14d AS
SELECT
    d.ref_identifier AS pickup_identifier,
    d.name,
    d.zip_code,
    COUNT(*) AS claims_last_14_days
FROM dw.fact_complaint f
JOIN dw.dim_pickup_station d
    ON d.pickup_station_id = f.pickup_station_id
    AND d.is_current = TRUE
WHERE f.complained_at >= (CURRENT_DATE - INTERVAL '14 days')
GROUP BY d.ref_identifier, d.name, d.zip_code;



---------- REPORT 2 ----------
-- Name and address of final recipients with more than 5 
-- complaints in the last two months and the given pickup 
-- locations of the according complaints per recipient
CREATE OR REPLACE VIEW dw.rpt_recipients_over5_complaints_2m AS
SELECT
    MIN(f.recipient_name)    AS recipient_name,                                                     -- final recipient name
    MIN(f.recipient_address) AS recipient_address,                                                  -- final recipient address
    COUNT(*)                 AS complaints_2m,                                                      -- number of complaints in the last two months
    STRING_AGG(                                                                                     -- concatenated name, ID and zip code of the pickup stations
        DISTINCT f.pickup_station_external_id, ' | ')
        FILTER (WHERE f.pickup_station_external_id IS NOT NULL) AS pickup_locations_given,
    STRING_AGG(                                                                                     -- concatenated name, ID and zip code enriched with reference ID pickup stations
        DISTINCT (
            d.ref_identifier || ' - ' || COALESCE(d.name,'') || ' (' || COALESCE(d.zip_code,'') || ')'
            ),' | '
        ) FILTER (WHERE d.pickup_station_id IS NOT NULL) AS pickup_locations_enriched
FROM dw.fact_complaint f
LEFT JOIN dw.dim_pickup_station d
    ON d.pickup_station_id = f.pickup_station_id
    AND d.is_current = TRUE
WHERE f.complained_at >= (CURRENT_DATE - INTERVAL '2 months')                                       -- filter for the last two months from the current date
GROUP BY
    f.recipient_name_norm,                                                                          -- aggregates by final recipient name
    f.recipient_address_norm                                                                        -- aggregates by final recipient address
    HAVING COUNT(*) > 5;                                                                            -- filters counts with more de 5 complaints
