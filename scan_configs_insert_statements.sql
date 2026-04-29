-- =====================================================================
-- CODE INSPECTOR — SAMPLE PROJECT CONFIGURATIONS
-- =====================================================================
-- 7 domain-specific projects with matching notebook files
-- Each project uses different rule/template/gate combinations
-- Upload notebooks to GitHub, then use these INSERTs in scan_configs_1
-- =====================================================================


-- ─────────────────────────────────────────────────────────────────────
-- REFERENCE: RULE IDs (from rule_library)
-- ─────────────────────────────────────────────────────────────────────
-- ID  | Rule Name                    | Category              | Severity
-- ----|------------------------------|-----------------------|---------
--  1  | Variable Naming Convention   | Code Quality          | MINOR
--  2  | File Naming Convention       | Code Quality          | MINOR
--  3  | Function Length              | Code Quality          | MAJOR
--  4  | Code Duplication             | Code Quality          | MINOR
--  5  | Cyclomatic Complexity        | Code Quality          | MAJOR
--  6  | Dead Code Detection          | Code Quality          | MINOR
--  7  | Semantic Naming              | Code Quality          | MINOR
--  8  | Hardcoded Credentials        | Security              | BLOCKER
--  9  | PII Exposure                 | Security              | CRITICAL
-- 10  | SQL Injection Risk           | Security              | CRITICAL
-- 11  | Connection String Exposure   | Security              | BLOCKER
-- 12  | Secrets Detection            | Security              | BLOCKER
-- 13  | Hardcoded File Paths         | Security              | MAJOR
-- 14  | Syntax Errors                | Syntax & Parsing      | BLOCKER
-- 15  | Parse Errors                 | Syntax & Parsing      | MAJOR
-- 16  | Notebook Syntax              | Notebook-Specific     | MAJOR
-- 17  | Unnecessary collect()        | Performance           | MAJOR
-- 18  | Broadcast Join Hints         | Performance           | MINOR
-- 19  | Partition Strategy           | Performance           | MINOR
-- 20  | Delta Optimization           | Performance           | MINOR
-- 21  | Magic Numbers                | Code Quality          | MINOR
-- 22  | Import Hygiene               | Code Quality          | MINOR
-- 23  | Docstring Coverage           | Documentation         | MINOR
-- 24  | Output Cleanup               | Notebook-Specific     | MINOR


-- ─────────────────────────────────────────────────────────────────────
-- REFERENCE: TEMPLATE IDs (from rule_templates)
-- ─────────────────────────────────────────────────────────────────────
-- ID | Template Name                  | Rule IDs Included
-- ---|--------------------------------|-------------------------------------------
--  1 | Security First                 | [8,9,10,11,12,13]
--  2 | PEP8 + Code Quality            | [1,2,3,4,5,6,7,21,22,23]
--  3 | Performance Optimization       | [17,18,19,20]
--  4 | Full Scan (All Rules)          | [1..24]
--  5 | Notebook Best Practices        | [14,15,16,24,23,3,6]
--  6 | Data Engineering Standard      | [1,3,5,8,11,13,14,17,19,20]
--  7 | Healthcare / HIPAA Compliance  | [8,9,10,11,12,13,23]
--  8 | Financial / SOX Compliance     | [8,9,10,11,12,13,3,5,23]
--  9 | Teradata Migration             | [1,3,8,10,11,13,14,15]
-- 10 | Minimal Quick Scan             | [8,11,12,14]
-- 11 | Documentation & Standards      | [1,2,7,22,23,24]
-- 12 | Full Migration Scan            | [1,2,3,5,8,9,10,11,12,13,14,15,16,17,21,22,23,24]


-- ─────────────────────────────────────────────────────────────────────
-- REFERENCE: GATE IDs (from quality_gates)
-- ─────────────────────────────────────────────────────────────────────
-- ID | Gate Condition             | Operator | Threshold | Severity
-- ---|----------------------------|----------|-----------|----------
--  1 | Minimum quality score      | >=       | 80        | block
--  2 | Zero blockers              | =        | 0         | block
--  3 | Maximum criticals          | <=       | 0         | warn
--  4 | Max technical debt (hrs)   | <=       | 4         | warn
--  5 | No new issues              | =        | 0         | warn
--  6 | Documentation coverage %   | >=       | 70        | info
--  7 | PII fields masked          | =        | 0         | block



-- ═════════════════════════════════════════════════════════════════════
-- PROJECT 1: PHARMA — Drug Trial Data Pipeline
-- ═════════════════════════════════════════════════════════════════════
-- Domain: Pharmaceutical / Clinical Trials
-- Focus:  PII (patient data), credentials, HIPAA-level security
-- Why:    Patient SSN, medication data, trial results — must be airtight
-- ─────────────────────────────────────────────────────────────────────

INSERT INTO `eagleeye-uc`.code_inspector_metadata.scan_configs_1
(project_name, project_desc, source_type, user_name, token, repository_name, branch_name,
 folders, file_names, schedule_time, schedule_frequency, start_date, end_date,
 repeat_frequency, start_time, end_time, selected_rules, selected_templates,
 selected_gates, is_active, created_by, created_at, updated_by, updated_at)
VALUES
('Pharma_Drug_Trial_ETL',
 'Clinical trial patient data ETL pipeline — PII-sensitive drug trial processing with HIPAA compliance requirements',
 'GitHub',
 '<your_github_username>',
 '<your_github_token>',
 '<your_repo_name>',
 'main',
 NULL,
 '["pharma_drug_trial_etl_pipeline.ipynb"]',
 '2026-04-29T06:00:00.000+00:00',
 'Daily',
 '2026-04-29T00:00:00.000+00:00',
 '2026-05-31T23:59:59.000+00:00',
 'Everyday',
 '-',
 '-',
 '[1,3,4,5,6,8,9,10,11,12,13,14,17,21,23]',
 '[1,7]',
 '[1,2,3,7]',
 true,
 NULL,
 CURRENT_TIMESTAMP(),
 NULL,
 CURRENT_TIMESTAMP());


-- ═════════════════════════════════════════════════════════════════════
-- PROJECT 2: BANKING — Loan Risk Scoring
-- ═════════════════════════════════════════════════════════════════════
-- Domain: Banking / Financial Services
-- Focus:  SOX compliance, credentials, PII (SSN, accounts), model governance
-- Why:    Credit decisions, customer financials — regulatory scrutiny
-- ─────────────────────────────────────────────────────────────────────

INSERT INTO `eagleeye-uc`.code_inspector_metadata.scan_configs_1
(project_name, project_desc, source_type, user_name, token, repository_name, branch_name,
 folders, file_names, schedule_time, schedule_frequency, start_date, end_date,
 repeat_frequency, start_time, end_time, selected_rules, selected_templates,
 selected_gates, is_active, created_by, created_at, updated_by, updated_at)
VALUES
('Banking_Loan_Risk_Model',
 'ML-based credit risk scoring for loan approvals — SOX-compliant with PII protection for customer financial data',
 'GitHub',
 '<your_github_username>',
 '<your_github_token>',
 '<your_repo_name>',
 'main',
 NULL,
 '["banking_loan_risk_scoring.ipynb"]',
 '2026-04-29T06:00:00.000+00:00',
 'Weekly',
 '2026-04-29T00:00:00.000+00:00',
 '2026-06-30T23:59:59.000+00:00',
 'Every Monday',
 '-',
 '-',
 '[1,2,3,4,5,6,8,9,10,11,12,13,14,15,16,17,21,22,23]',
 '[8,2]',
 '[1,2,3,4,7]',
 true,
 NULL,
 CURRENT_TIMESTAMP(),
 NULL,
 CURRENT_TIMESTAMP());


-- ═════════════════════════════════════════════════════════════════════
-- PROJECT 3: TELECOM — Churn Prediction Pipeline
-- ═════════════════════════════════════════════════════════════════════
-- Domain: Telecommunications
-- Focus:  Code quality (terrible naming), credentials, IMEI/phone PII
-- Why:    Customer 360 data, GDPR-sensitive, bad code practices demo
-- ─────────────────────────────────────────────────────────────────────

INSERT INTO `eagleeye-uc`.code_inspector_metadata.scan_configs_1
(project_name, project_desc, source_type, user_name, token, repository_name, branch_name,
 folders, file_names, schedule_time, schedule_frequency, start_date, end_date,
 repeat_frequency, start_time, end_time, selected_rules, selected_templates,
 selected_gates, is_active, created_by, created_at, updated_by, updated_at)
VALUES
('Telecom_Churn_Prediction',
 'Customer churn prediction pipeline — usage pattern analysis with GDPR-compliant data handling for telecom subscriber data',
 'GitHub',
 '<your_github_username>',
 '<your_github_token>',
 '<your_repo_name>',
 'main',
 NULL,
 '["telecom_churn_prediction_pipeline.ipynb"]',
 '2026-04-29T08:00:00.000+00:00',
 'Daily',
 '2026-04-29T00:00:00.000+00:00',
 '2026-05-31T23:59:59.000+00:00',
 'Everyday',
 '-',
 '-',
 '[1,3,4,5,6,7,8,9,11,12,13,14,16,17,21]',
 '[2,1]',
 '[1,2,3]',
 true,
 NULL,
 CURRENT_TIMESTAMP(),
 NULL,
 CURRENT_TIMESTAMP());


-- ═════════════════════════════════════════════════════════════════════
-- PROJECT 4: HEALTHCARE — OMOP Patient Analytics
-- ═════════════════════════════════════════════════════════════════════
-- Domain: Healthcare / Hospital System
-- Focus:  HIPAA compliance, PHI exposure, OMOP CDM naming, encryption
-- Why:    Patient medical records, SSN, diagnosis data — highest stakes
-- ─────────────────────────────────────────────────────────────────────

INSERT INTO `eagleeye-uc`.code_inspector_metadata.scan_configs_1
(project_name, project_desc, source_type, user_name, token, repository_name, branch_name,
 folders, file_names, schedule_time, schedule_frequency, start_date, end_date,
 repeat_frequency, start_time, end_time, selected_rules, selected_templates,
 selected_gates, is_active, created_by, created_at, updated_by, updated_at)
VALUES
('Healthcare_OMOP_Analytics',
 'OMOP CDM patient outcome analytics — HIPAA-mandated PHI/PII protection with SOC2-compliant quality gates for hospital data platform',
 'GitHub',
 '<your_github_username>',
 '<your_github_token>',
 '<your_repo_name>',
 'main',
 NULL,
 '["healthcare_omop_patient_analytics.ipynb"]',
 '2026-04-29T06:00:00.000+00:00',
 'Daily',
 '2026-04-29T00:00:00.000+00:00',
 '2026-05-31T23:59:59.000+00:00',
 'Everyday',
 '-',
 '-',
 '[1,2,3,4,5,6,8,9,10,11,12,13,14,15,16,17,21,22,23,24]',
 '[7,4]',
 '[1,2,3,4,6,7]',
 true,
 NULL,
 CURRENT_TIMESTAMP(),
 NULL,
 CURRENT_TIMESTAMP());


-- ═════════════════════════════════════════════════════════════════════
-- PROJECT 5: RETAIL — Demand Forecasting
-- ═════════════════════════════════════════════════════════════════════
-- Domain: Retail / E-Commerce
-- Focus:  Data engineering quality, performance, customer PII
-- Why:    Transaction data, loyalty card info, ML model governance
-- ─────────────────────────────────────────────────────────────────────

INSERT INTO `eagleeye-uc`.code_inspector_metadata.scan_configs_1
(project_name, project_desc, source_type, user_name, token, repository_name, branch_name,
 folders, file_names, schedule_time, schedule_frequency, start_date, end_date,
 repeat_frequency, start_time, end_time, selected_rules, selected_templates,
 selected_gates, is_active, created_by, created_at, updated_by, updated_at)
VALUES
('Retail_Demand_Forecast',
 'ML demand forecasting and inventory optimization — customer transaction analytics with PCI-DSS compliant payment data handling',
 'GitHub',
 '<your_github_username>',
 '<your_github_token>',
 '<your_repo_name>',
 'main',
 NULL,
 '["retail_demand_forecasting.ipynb"]',
 '2026-04-29T07:00:00.000+00:00',
 'Weekly',
 '2026-04-29T00:00:00.000+00:00',
 '2026-06-30T23:59:59.000+00:00',
 'Every Monday',
 '-',
 '-',
 '[1,3,4,6,7,8,9,11,12,13,14,16,17,21]',
 '[6,3]',
 '[1,2,3]',
 true,
 NULL,
 CURRENT_TIMESTAMP(),
 NULL,
 CURRENT_TIMESTAMP());


-- ═════════════════════════════════════════════════════════════════════
-- PROJECT 6: INSURANCE — Claims Fraud Detection
-- ═════════════════════════════════════════════════════════════════════
-- Domain: Insurance
-- Focus:  Full scan — every rule category, strictest gates
-- Why:    Claims data, SSN, financial fraud — regulatory + model risk
-- ─────────────────────────────────────────────────────────────────────

INSERT INTO `eagleeye-uc`.code_inspector_metadata.scan_configs_1
(project_name, project_desc, source_type, user_name, token, repository_name, branch_name,
 folders, file_names, schedule_time, schedule_frequency, start_date, end_date,
 repeat_frequency, start_time, end_time, selected_rules, selected_templates,
 selected_gates, is_active, created_by, created_at, updated_by, updated_at)
VALUES
('Insurance_Fraud_Detection',
 'ML claims fraud detection pipeline — full regulatory scan with PII protection for policyholder and claimant sensitive data',
 'GitHub',
 '<your_github_username>',
 '<your_github_token>',
 '<your_repo_name>',
 'main',
 NULL,
 '["insurance_claims_fraud_detection.ipynb"]',
 '2026-04-29T06:00:00.000+00:00',
 'Daily',
 '2026-04-29T00:00:00.000+00:00',
 '2026-05-31T23:59:59.000+00:00',
 'Everyday',
 '-',
 '-',
 '[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,21,22,23,24]',
 '[4,1,8]',
 '[1,2,3,4,5,6,7]',
 true,
 NULL,
 CURRENT_TIMESTAMP(),
 NULL,
 CURRENT_TIMESTAMP());


-- ═════════════════════════════════════════════════════════════════════
-- PROJECT 7: ENERGY — Smart Grid Anomaly Detection
-- ═════════════════════════════════════════════════════════════════════
-- Domain: Energy / Utilities
-- Focus:  Performance (IoT sensor data), data engineering, security
-- Why:    SCADA systems, real-time sensor streams, critical infra
-- ─────────────────────────────────────────────────────────────────────

INSERT INTO `eagleeye-uc`.code_inspector_metadata.scan_configs_1
(project_name, project_desc, source_type, user_name, token, repository_name, branch_name,
 folders, file_names, schedule_time, schedule_frequency, start_date, end_date,
 repeat_frequency, start_time, end_time, selected_rules, selected_templates,
 selected_gates, is_active, created_by, created_at, updated_by, updated_at)
VALUES
('Energy_Grid_Anomaly_Detection',
 'Smart grid sensor anomaly detection — predictive maintenance pipeline for power grid with SCADA security compliance',
 'GitHub',
 '<your_github_username>',
 '<your_github_token>',
 '<your_repo_name>',
 'main',
 NULL,
 '["energy_smart_grid_anomaly_detection.ipynb"]',
 '2026-04-29T05:00:00.000+00:00',
 'Daily',
 '2026-04-29T00:00:00.000+00:00',
 '2026-05-31T23:59:59.000+00:00',
 'Everyday',
 '-',
 '-',
 '[1,3,5,6,8,11,12,13,14,16,17,19,20,21]',
 '[6,3]',
 '[1,2,4]',
 true,
 NULL,
 CURRENT_TIMESTAMP(),
 NULL,
 CURRENT_TIMESTAMP());



-- ═════════════════════════════════════════════════════════════════════
-- EXPECTED ISSUES PER NOTEBOOK (what the inspector SHOULD catch)
-- ═════════════════════════════════════════════════════════════════════
--
-- ┌──────────────────────────┬──────────┬──────────┬──────────┬──────────┬──────────┬──────────┬──────────┐
-- │ Rule                     │ Pharma   │ Banking  │ Telecom  │ Health   │ Retail   │ Insure   │ Energy   │
-- ├──────────────────────────┼──────────┼──────────┼──────────┼──────────┼──────────┼──────────┼──────────┤
-- │ Hardcoded Credentials    │ ██ 2     │ ██ 3     │ ██ 3     │ ██ 3     │ ██ 3     │ ██ 4     │ ██ 2     │
-- │ PII Exposure             │ ██ 2     │ ██ 2     │ ██ 2     │ ██ 3     │ ██ 1     │ ██ 2     │ █ 1      │
-- │ SQL Injection            │ █ 1      │ █ 1      │ █ 1      │ █ 1      │ █ 1      │ █ 1      │ -        │
-- │ Connection Strings       │ █ 1      │ █ 1      │ █ 1      │ █ 1      │ █ 1      │ █ 1      │ -        │
-- │ Hardcoded Paths          │ █ 1      │ █ 1      │ █ 1      │ -        │ █ 1      │ █ 1      │ █ 1      │
-- │ Function Length          │ █ 1      │ █ 1      │ █ 1      │ █ 1      │ -        │ █ 1      │ -        │
-- │ Code Duplication         │ █ 1      │ █ 1      │ █ 1      │ █ 1      │ █ 1      │ █ 1      │ █ 1      │
-- │ Dead Code                │ ██ 3     │ ██ 3     │ ██ 3     │ ██ 3     │ ██ 2     │ ██ 3     │ ██ 2     │
-- │ Magic Numbers            │ ██ 8+    │ ██ 6+    │ ██ 6+    │ ██ 6+    │ ██ 5+    │ ██ 8+    │ ██ 5+    │
-- │ Variable Naming          │ █ 1      │ ██ 3     │ ██ 5+    │ █ 1      │ ██ 3     │ ██ 3     │ -        │
-- │ Complexity               │ ██ 2     │ █ 1      │ ██ 2     │ █ 1      │ -        │ █ 1      │ -        │
-- │ collect()                │ █ 1      │ █ 1      │ █ 1      │ █ 1      │ █ 1      │ █ 1      │ █ 1      │
-- │ Notebook Syntax          │ ✓        │ ✓        │ ✓        │ ✓        │ ✓        │ ✓        │ ✓        │
-- ├──────────────────────────┼──────────┼──────────┼──────────┼──────────┼──────────┼──────────┼──────────┤
-- │ Est. Total Issues        │ ~24      │ ~27      │ ~29      │ ~26      │ ~22      │ ~30      │ ~16      │
-- │ Expected Grade           │ D-F      │ D-F      │ F        │ D-F      │ D        │ F        │ C-D      │
-- │ Gate: Pass/Fail          │ FAIL     │ FAIL     │ FAIL     │ FAIL     │ FAIL     │ FAIL     │ FAIL     │
-- └──────────────────────────┴──────────┴──────────┴──────────┴──────────┴──────────┴──────────┴──────────┘
--
-- All notebooks are intentionally "bad" to generate rich scan results.
-- Every notebook has at least: credentials, PII, magic numbers, collect(), duplication.
-- Telecom notebook has the WORST variable naming (a, b, c, d, e, f, g).
-- Insurance notebook has the MOST rules enabled (full scan) and ALL 7 gates.
-- Energy notebook is the "cleanest" — fewer issues, best for showing partial passes.
