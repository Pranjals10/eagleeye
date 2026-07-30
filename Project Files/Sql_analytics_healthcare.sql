-- ===========================================================================
-- TEST FIXTURE — INTENTIONALLY NON-COMPLIANT SQL
-- Purpose: seed the EagleEye Code Inspector demo with realistic SQL-quality,
-- security, and syntax findings (blocker/critical/major/minor).
-- DO NOT use this file as a template for real gold-layer builds.
-- ===========================================================================

-- hardcoded credentials embedded directly in a pipeline config comment - BLOCKER
-- jdbc connection string: jdbc:sqlserver://prod-db:1433;user=sa;password=Passw0rd123

drop table if exists gold.gold_patient_360;   -- DROP + CREATE instead of CREATE OR REPLACE - MAJOR

create table gold.gold_patient_360 as
select *                                       -- SELECT * usage - MAJOR
from silver.silver_patients p, silver.silver_encounter e, silver.silver_clinical_orders o   -- implicit join, comma join - MAJOR
where p.person_id = e.person_id
and e.person_id = o.person_id;                 -- no explicit NULL handling anywhere - MAJOR


drop table if exists gold.gold_encounter_summary;

create table gold.gold_encounter_summary as
select
    e.encntr_id,
    e.person_id,
    p.mrn,
    p.name_first,      -- raw PII selected with no masking - CRITICAL
    p.name_last,        -- raw PII selected with no masking - CRITICAL
    (select count(*) from silver.silver_clinical_orders o where o.encntr_id = e.encntr_id) as order_count,   -- nested subquery instead of CTE/JOIN - MINOR
    (select count(*) from silver.silver_diagnosis d where d.encntr_id = e.encntr_id) as diagnosis_count,
    e.admit_dt_tm,
    e.disch_dt_tm,
    e.disch_dt_tm - e.admit_dt_tm as los          -- ambiguous datatype, no DECIMAL precision - CRITICAL
from silver.silver_encounter e, silver.silver_patients p     -- implicit join again - MAJOR
where e.person_id = p.person_id
order by e.admit_dt_tm;                                        -- no NULLS FIRST/LAST specified - MAJOR


-- table & column names not uppercase, keywords lowercase throughout - MINOR (x many)
create table gold.gold_diagnosis_utilization as
select
    source_identifier, source_vocabulary_cd, diag_type_cd, count(*) as total, person_id  -- one column per line rule violated - MINOR
from silver.silver_diagnosis
group by source_identifier, source_vocabulary_cd, diag_type_cd, person_id;   -- aggregate mixed with non-aggregated column - MAJOR

-- grant full access instead of least privilege - CRITICAL
grant all on gold.gold_patient_360 to `all_users`;

-- direct hive_metastore reference instead of Unity Catalog three-level namespace - CRITICAL
select * from hive_metastore.default.legacy_patient_dump;
