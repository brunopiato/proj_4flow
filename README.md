# 4Flow Data Engineering Case – Parcel Service Monitoring

## 1. Context and Objective

The company operates exclusively with deliveries to pickup stations and has recently observed an increase in customer complaints related to late or missing deliveries. The root causes are unclear.

The objective of this solution is to design and implement a data pipeline and analytical model that enables:

- Monitoring pickup stations with a high number of complaints
- Identifying recipients with recurring complaints
- Handling inconsistent pickup station identifiers across data sources
- Preserving historical changes in pickup station master data
- Keeping delivery events available for additional operational monitoring (e.g., trends, drivers, parcel/PO linkage)

---

## 2. Solution Overview

The solution is built on top of a relational Data Warehouse (PostgreSQL) and follows a layered architecture:

- Landing layer: file ingestion control
- Staging layer: normalized tabular data parsed from CSV, JSON, and XML
- Data Warehouse (DW):
    - Historical pickup station dimension (SCD Type 2)
    - Complaint fact table
    - Delivery event fact table
    - Reporting views

The pipeline runs daily, ensuring idempotent ingestion, traceability, and consistent reporting.

---

## 3. Data Model (High-Level)

### 3.1 Dimension – dw.dim_pickup_station (SCD Type 2)

This dimension stores the historical state of pickup stations over time.

Key characteristics:

- Slowly Changing Dimension Type 2
- pickup_station_id as the internal canonical identifier
- pickup_station_sk as the surrogate key per version
- Validity tracking (valid_from, valid_to, is_current)
- is_active flag for deactivated stations
- Deterministic fingerprint (e.g. zip code + street) to identify the same station when external identifiers change

### 3.2 Fact – dw.fact_complaint

Stores individual complaint events.

Key characteristics:

- One row per complaint
- Deduplication via record_hash_sha256
- pickup_station_external_id preserved exactly as received
- pickup_station_id populated later via enrichment when a reliable match exists
- Complaints without a valid pickup station match are not discarded

### 3.3 Fact – dw.fact_delivery_event

Stores individual delivery events created by drivers when parcels are delivered to pickup stations.

Key characteristics:

- One row per delivery event
- Deduplication via record_hash_sha256
- Stores operational identifiers and event context: driver_id, parcel_id, purchase_order_id
- Stores pickup_station_external_id as received in delivery event files
- No reliable mapping to dw.dim_pickup_station in the simplified scope (identifier inconsistency), therefore pickup_station_id is not used for this dataset in the current design
- Still useful for operational monitoring and correlation with complaints via parcel_id/purchase_order_id when present

---

## 4. Data Pipeline (ETL)

The daily pipeline executes the following steps:

### 4.1 Source → Landing
- Files arrive via a central file server (CSV, JSON, XML)
- Each file is registered in landing.landing_file
- Prevents reprocessing of the same file

### 4.2 Landing → Staging
- Format-specific parsers (CSV, JSON, XML)
- Field normalization
- Generation of record_hash_sha256
- Insertion into:
    - staging.stg_complaint
    - staging.stg_delivery_event

### 4.3 Staging → Data Warehouse

#### 4.3.1 Pickup Station Dimension Update

- Daily snapshot of the reference pickup station table
- Application of SCD2 logic:
    - ADD: new stations
    - UPDATE: attribute or identifier changes
    - REMOVE: stations removed from the reference (marked inactive)

#### 4.3.2 Complaint Fact Load

- Phase 1: raw insert from staging.stg_complaint into dw.fact_complaint (idempotent)
- Phase 2: enrichment of pickup_station_id using the current pickup station dimension (direct match on complaint pickup station identifier in the simplified scenario)

#### 4.3.3 Delivery Event Fact Load

- Raw insert from staging.stg_delivery_event into dw.fact_delivery_event (idempotent)
- No pickup station dimension enrichment is performed for delivery events in this simplified scope due to inconsistent identifiers

---

## 5. Delivered Reports

### 5.1 Report 1 – Pickup Station Complaint Hotspots (Last 14 Days)

View: *dw.rpt_pickup_station_claims_14d*

Displays:

- Pickup station identifier
- Name
- Zip code
- Number of complaints in the last 14 days

Only complaints successfully linked to the pickup station dimension are included.

### 5.2 Report 2 – Recipients with More Than 5 Complaints (Last 2 Months)

View: *dw.rpt_recipients_over5_complaints_2m*

Displays:

- Recipient name and address
- Total number of complaints in the last two months
- Pickup locations associated with the recipient’s complaints:
    - Given pickup locations (as received in the complaint)
    - Enriched pickup locations (when linked to the dimension)

Each row represents one recipient, aggregated across all their complaints.

---

## 6. Assumptions and Limitations

1.	Complaints are the primary data source for the requested reports.
2.	Pickup station identifiers in complaints can be directly matched to the reference list (simplified scenario).
3.	Pickup station identifiers in delivery events are not compatible with the reference list and are not linked to the pickup station dimension in this scope.
4.	No crosswalk table is maintained between external systems.
5.	A simple fingerprint is sufficient to identify the same pickup station over time in the reference-driven dimension build.
6.	Ambiguous fingerprint matches are out of scope.
7.	The pipeline runs daily.
8.	Data ingestion is idempotent.
9.	Failed pickup station matches do not block complaint ingestion.
10.	Reports are delivered in aggregated form (no per-event drill-down in scope).

---

## 7. Technical Justification

- PostgreSQL was chosen for simplicity and strong analytical SQL support.
- SCD Type 2 enables full historical tracking of pickup station changes.
- Separating ingestion from enrichment improves robustness and debuggability.
- Keeping dw.fact_delivery_event provides operational visibility and enables future correlation analysis (e.g., by parcel_id/purchase_order_id) even without a pickup station dimension join.
- Reporting views provide a clean interface for BI tools or end users.
- The modular pipeline design allows future extensions (e.g. adding a crosswalk, improving matching, or adding more data quality checks).

---

## 8. Conclusion

The proposed solution addresses the business problem by providing:

- Clear visibility into pickup station complaint hotspots
- Identification of problematic recipients
- Explicit handling of identifier inconsistencies
- Reliable historical tracking of pickup station master data
- A structured foundation for delivery-event monitoring and future complaint-delivery correlation

All while maintaining simplicity, clarity, and robustness, aligned with the scope of the technical case.

---

## 9. Further steps

- Monitoring
    - AImplement pipeline-level monitoring to track ingestion volumes, processing success, and data quality metrics across stages.

-  Complaint–delivery correlation
    - Link complaints to delivery events via *parcel_id* or *purchase_order_id* whenever its possible to analyze delays and missing deliveries.

- Complaint rate analysis
    - Compare complaint counts against delivery volumes to detect abnormal complaint ratios.

- Driver-level insights
    - Identify drivers associated with higher complaint frequency.

- Recipient behavior patterns
    - Analyze delivery frequency versus complaint frequency per recipient.

- Pickup station mapping improvements
    - Introduce crosswalk or probabilistic matching to link delivery events to pickup stations.

- Operational monitoring and alerts
    - Define SLAs and trigger alerts when complaint thresholds are exceeded.