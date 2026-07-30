-- =============================================================================
-- Gold Layer Build - Healthcare Domain
-- Consumes silver tables: silver_patients, silver_encounters,
-- silver_clinical_events, silver_diagnosis_enriched, silver_clinical_orders,
-- silver_patient_visits
--
-- NOTE: Seeded test fixture for a code-quality inspector. Contains a mix of
-- compliant and rule-violating SQL across formatting, join style, security,
-- and syntax/parsing categories to validate scan coverage.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- GOLD TABLE 1: gold_patient_encounter_summary  (COMPLIANT QUERY)
-- Uppercase keywords, explicit JOIN syntax, CTE, meaningful aliases,
-- explicit NULL handling, DECIMAL precision specified.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE main.gold.gold_patient_encounter_summary (
    PERSON_ID           BIGINT NOT NULL,
    MRN                 STRING NOT NULL,
    ENCOUNTER_COUNT      INT,
    LAST_ADMIT_DT_TM     TIMESTAMP,
    TOTAL_CHARGE_AMOUNT   DECIMAL(18,2),
    CONSTRAINT PK_GOLD_PATIENT_ENCOUNTER_SUMMARY PRIMARY KEY (PERSON_ID)
);

WITH ENCOUNTER_AGG AS (
    SELECT
        E.PERSON_ID,
        COUNT(E.ENCNTR_ID) AS ENCOUNTER_COUNT,
        MAX(E.ADMIT_DT_TM) AS LAST_ADMIT_DT_TM
    FROM MAIN.SILVER.SILVER_ENCOUNTERS AS E
    WHERE E.ACTIVE_IND = 1
    GROUP BY E.PERSON_ID
)
INSERT OVERWRITE TABLE MAIN.GOLD.GOLD_PATIENT_ENCOUNTER_SUMMARY
SELECT
    P.PERSON_ID,
    P.MRN,
    COALESCE(EA.ENCOUNTER_COUNT, 0) AS ENCOUNTER_COUNT,
    EA.LAST_ADMIT_DT_TM,
    CAST(0.00 AS DECIMAL(18,2)) AS TOTAL_CHARGE_AMOUNT
FROM MAIN.SILVER.SILVER_PATIENTS AS P
INNER JOIN ENCOUNTER_AGG AS EA
    ON P.PERSON_ID = EA.PERSON_ID
ORDER BY P.PERSON_ID NULLS LAST;


-- -----------------------------------------------------------------------------
-- GOLD TABLE 2: gold_clinical_orders_summary  (VIOLATION-HEAVY QUERY)
-- Lowercase keywords, SELECT *, implicit comma join, single-letter aliases,
-- no NULL handling, missing constraints, missing DECIMAL precision.
-- -----------------------------------------------------------------------------
create table main.gold.gold_clinical_orders_summary
using delta
location '/mnt/external/gold/clinical_orders_summary'  -- VIOLATION: external LOCATION instead of managed table
as
select *  -- VIOLATION: SELECT * instead of explicit columns
from main.silver.silver_clinical_orders o, main.silver.silver_diagnosis_enriched d  -- VIOLATION: implicit comma join
where o.person_id = d.person_id
  and o.active_ind = 1;

alter table main.gold.gold_clinical_orders_summary
add column total_cost decimal;  -- VIOLATION: DECIMAL missing precision/scale

grant all privileges on table main.gold.gold_clinical_orders_summary to `all_users`;  -- VIOLATION: least-privilege violated (GRANT ALL)


-- -----------------------------------------------------------------------------
-- GOLD TABLE 3: gold_diagnosis_trends  (MIXED: some compliant, some violating)
-- Uses Unity Catalog namespace (correct) but references legacy hive_metastore
-- in a comment-adjacent join, and includes an un-mapped Teradata-style
-- QUALIFY usage left in place to test syntax/parsing detection.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE main.gold.gold_diagnosis_trends (
    DIAGNOSIS_ID        BIGINT NOT NULL,
    PERSON_ID            BIGINT NOT NULL,
    DIAG_TYPE_CD          STRING,
    CLINICAL_DIAG_DT_TM     TIMESTAMP,
    RANK_WITHIN_PERSON      INT,
    CONSTRAINT PK_GOLD_DIAGNOSIS_TRENDS PRIMARY KEY (DIAGNOSIS_ID)
);

-- correct: CTE + explicit JOIN + uppercase keywords
WITH RANKED_DIAGNOSIS AS (
    SELECT
        D.DIAGNOSIS_ID,
        D.PERSON_ID,
        D.DIAG_TYPE_CD,
        D.CLINICAL_DIAG_DT_TM,
        ROW_NUMBER() OVER (
            PARTITION BY D.PERSON_ID
            ORDER BY D.CLINICAL_DIAG_DT_TM DESC
        ) AS RANK_WITHIN_PERSON
    FROM MAIN.SILVER.SILVER_DIAGNOSIS_ENRICHED AS D
    INNER JOIN MAIN.SILVER.SILVER_PATIENTS AS P
        ON D.PERSON_ID = P.PERSON_ID
    WHERE D.DIAG_CONFIRMED_IND = 1
)
INSERT OVERWRITE TABLE MAIN.GOLD.GOLD_DIAGNOSIS_TRENDS
SELECT DIAGNOSIS_ID, PERSON_ID, DIAG_TYPE_CD, CLINICAL_DIAG_DT_TM, RANK_WITHIN_PERSON
FROM RANKED_DIAGNOSIS
QUALIFY RANK_WITHIN_PERSON = 1;  -- VIOLATION: Teradata QUALIFY not converted to Spark SQL subquery/WHERE pattern

-- VIOLATION: legacy hive_metastore reference left in a maintenance query below
SELECT COUNT(*) FROM hive_metastore.legacy_bronze.diagnosis_archive;
