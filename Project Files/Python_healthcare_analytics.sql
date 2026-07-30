-- =============================================================================
-- Gold Layer Build - Healthcare Domain
-- Consumes silver tables: silver_patients, silver_encounters,
-- silver_clinical_events, silver_diagnosis_enriched, silver_clinical_orders,
-- silver_patient_visits
--
-- Standards followed: uppercase SQL keywords, explicit JOIN syntax, CTEs for
-- multi-step logic, meaningful table aliases, explicit NULL handling,
-- DECIMAL columns with explicit precision/scale, PK/NOT NULL constraints,
-- Unity Catalog three-level namespacing, and least-privilege grants.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- GOLD TABLE 1: gold_patient_encounter_summary
-- Per-patient encounter counts and most recent admission.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE main.gold.gold_patient_encounter_summary (
    PERSON_ID            BIGINT NOT NULL,
    MRN                  STRING NOT NULL,
    ENCOUNTER_COUNT       INT,
    LAST_ADMIT_DT_TM      TIMESTAMP,
    TOTAL_CHARGE_AMOUNT    DECIMAL(18,2),
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
-- GOLD TABLE 2: gold_clinical_orders_summary
-- Active clinical orders joined with enriched diagnosis records.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE main.gold.gold_clinical_orders_summary (
    ORDER_ID             BIGINT NOT NULL,
    PERSON_ID            BIGINT NOT NULL,
    DIAGNOSIS_ID          BIGINT,
    ORDER_MNEMONIC        STRING,
    ORDER_DTM             TIMESTAMP,
    TOTAL_COST            DECIMAL(18,2),
    CONSTRAINT PK_GOLD_CLINICAL_ORDERS_SUMMARY PRIMARY KEY (ORDER_ID)
);

WITH ACTIVE_ORDERS AS (
    SELECT
        O.ORDER_ID,
        O.PERSON_ID,
        O.ORDER_MNEMONIC,
        O.ORDER_DTM
    FROM MAIN.SILVER.SILVER_CLINICAL_ORDERS AS O
    WHERE O.ACTIVE_IND = 1
)
INSERT OVERWRITE TABLE MAIN.GOLD.GOLD_CLINICAL_ORDERS_SUMMARY
SELECT
    AO.ORDER_ID,
    AO.PERSON_ID,
    D.DIAGNOSIS_ID,
    AO.ORDER_MNEMONIC,
    AO.ORDER_DTM,
    CAST(0.00 AS DECIMAL(18,2)) AS TOTAL_COST
FROM ACTIVE_ORDERS AS AO
LEFT JOIN MAIN.SILVER.SILVER_DIAGNOSIS_ENRICHED AS D
    ON AO.PERSON_ID = D.PERSON_ID;

GRANT SELECT ON TABLE main.gold.gold_clinical_orders_summary TO `analytics_readers`;


-- -----------------------------------------------------------------------------
-- GOLD TABLE 3: gold_diagnosis_trends
-- Most recent confirmed diagnosis per patient, ranked with a window function.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE main.gold.gold_diagnosis_trends (
    DIAGNOSIS_ID         BIGINT NOT NULL,
    PERSON_ID            BIGINT NOT NULL,
    DIAG_TYPE_CD          STRING,
    CLINICAL_DIAG_DT_TM     TIMESTAMP,
    RANK_WITHIN_PERSON      INT,
    CONSTRAINT PK_GOLD_DIAGNOSIS_TRENDS PRIMARY KEY (DIAGNOSIS_ID)
);

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
),
LATEST_DIAGNOSIS AS (
    SELECT
        DIAGNOSIS_ID,
        PERSON_ID,
        DIAG_TYPE_CD,
        CLINICAL_DIAG_DT_TM,
        RANK_WITHIN_PERSON
    FROM RANKED_DIAGNOSIS
    WHERE RANK_WITHIN_PERSON = 1
)
INSERT OVERWRITE TABLE MAIN.GOLD.GOLD_DIAGNOSIS_TRENDS
SELECT DIAGNOSIS_ID, PERSON_ID, DIAG_TYPE_CD, CLINICAL_DIAG_DT_TM, RANK_WITHIN_PERSON
FROM LATEST_DIAGNOSIS;
