-- ===========================================================================
-- gold_layer_ddl.sql
-- Gold-layer tables built from the silver healthcare tables.
-- Kept fully compliant with SQL quality, security, and governance rules.
-- ===========================================================================

CREATE DATABASE IF NOT EXISTS gold;

CREATE OR REPLACE TABLE gold.gold_patient_360 AS
WITH encounter_rollup AS (
    SELECT
        person_id,
        COUNT(DISTINCT encntr_id) AS total_encounters,
        MAX(admit_dt_tm) AS most_recent_admit_dt_tm
    FROM silver.silver_encounter
    GROUP BY person_id
),
diagnosis_rollup AS (
    SELECT
        person_id,
        COUNT(DISTINCT diagnosis_id) AS total_diagnoses,
        SUM(CASE WHEN is_confirmed THEN 1 ELSE 0 END) AS confirmed_diagnoses
    FROM silver.silver_diagnosis
    GROUP BY person_id
)
SELECT
    patients.person_id,
    patients.mrn,
    patients.sex_desc,
    COALESCE(encounter_rollup.total_encounters, 0) AS total_encounters,
    encounter_rollup.most_recent_admit_dt_tm,
    COALESCE(diagnosis_rollup.total_diagnoses, 0) AS total_diagnoses,
    COALESCE(diagnosis_rollup.confirmed_diagnoses, 0) AS confirmed_diagnoses,
    CURRENT_TIMESTAMP() AS gold_load_ts
FROM silver.silver_patients AS patients
LEFT JOIN encounter_rollup
    ON patients.person_id = encounter_rollup.person_id
LEFT JOIN diagnosis_rollup
    ON patients.person_id = diagnosis_rollup.person_id;


CREATE OR REPLACE TABLE gold.gold_encounter_summary AS
SELECT
    encounter.encntr_id,
    encounter.person_id,
    patients.mrn,
    encounter.admit_dt_tm,
    encounter.disch_dt_tm,
    COALESCE(order_counts.order_count, 0) AS order_count,
    CURRENT_TIMESTAMP() AS gold_load_ts
FROM silver.silver_encounter AS encounter
LEFT JOIN silver.silver_patients AS patients
    ON encounter.person_id = patients.person_id
LEFT JOIN (
    SELECT
        encntr_id,
        COUNT(DISTINCT order_id) AS order_count
    FROM silver.silver_clinical_orders
    GROUP BY encntr_id
) AS order_counts
    ON encounter.encntr_id = order_counts.encntr_id
ORDER BY encounter.admit_dt_tm NULLS LAST;
