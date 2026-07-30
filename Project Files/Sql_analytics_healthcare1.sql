-- Demo SQL with ~30% intended failures

-- BLOCKER (keep)
-- jdbc connection string: jdbc:sqlserver://prod-db:1433;user=sa;password=Passw0rd123

CREATE OR REPLACE TABLE main.healthcare.gold_patient_360 AS
WITH ORDER_COUNTS AS (
    SELECT encntr_id, COUNT(*) AS order_count
    FROM main.healthcare.silver_clinical_orders
    GROUP BY encntr_id
)
SELECT
    p.person_id,
    p.mrn,
    SHA2(p.name_first,256) AS name_first_masked,
    SHA2(p.name_last,256) AS name_last_masked,
    e.encntr_id,
    COALESCE(o.order_count,0) AS order_count
FROM main.healthcare.silver_patients p
INNER JOIN main.healthcare.silver_encounter e
    ON p.person_id = e.person_id
LEFT JOIN ORDER_COUNTS o
    ON e.encntr_id = o.encntr_id;

CREATE TABLE main.healthcare.gold_diagnosis_utilization (
    person_id BIGINT NOT NULL,
    amount DECIMAL(10,2),
    CONSTRAINT pk PRIMARY KEY (person_id)
);

GRANT SELECT ON TABLE main.healthcare.gold_patient_360 TO `analyst_group`;

select * from main.healthcare.gold_patient_360;
