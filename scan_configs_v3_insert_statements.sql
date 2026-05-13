-- =====================================================================
-- CODE INSPECTOR v3 — 12 DOMAIN NOTEBOOKS (4 per domain)
-- =====================================================================
-- ALL 12 notebooks verified: ✅ ALL PATTERNS match scanner regex
-- Each notebook triggers 45-55+ total issues across all 24 rules
-- Replace <your_github_username>, <your_github_token>, <your_repo_name>
-- =====================================================================


-- ═════════════════════════════════════════════════════════════════════
--  RETAIL DOMAIN (4 notebooks)
-- ═════════════════════════════════════════════════════════════════════

-- R1: Sales Transaction ETL Pipeline
INSERT INTO `eagleeye-uc`.code_inspector_metadata.scan_configs_1
(project_name, project_desc, source_type, user_name, token, repository_name, branch_name,
 folders, file_names, schedule_time, schedule_frequency, start_date, end_date,
 repeat_frequency, start_time, end_time, selected_rules, selected_templates,
 selected_gates, is_active, created_by, created_at, updated_by, updated_at)
VALUES
('Retail_Sales_Transaction_ETL',
 'POS transaction ETL pipeline — extract, cleanse, deduplicate, Delta load. PCI-DSS, PII, fraud flag scanning.',
 'GitHub',
 '<your_github_username>',
 '<your_github_token>',
 '<your_repo_name>',
 'main',
 NULL,
 ARRAY('retail_nb_sales_transaction_etl.ipynb'),
 '2026-05-14T06:00:00.000+00:00',
 'Daily',
 '2026-05-14T00:00:00.000+00:00',
 '2026-06-30T23:59:59.000+00:00',
 'Everyday',
 '-',
 '-',
 ARRAY(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,21,22,23,24),
 ARRAY(4,1,2),
 ARRAY(1,2,3,4,7),
 true,
 NULL,
 CURRENT_TIMESTAMP(),
 NULL,
 CURRENT_TIMESTAMP());


-- R2: Customer Segmentation & CLV
INSERT INTO `eagleeye-uc`.code_inspector_metadata.scan_configs_1
(project_name, project_desc, source_type, user_name, token, repository_name, branch_name,
 folders, file_names, schedule_time, schedule_frequency, start_date, end_date,
 repeat_frequency, start_time, end_time, selected_rules, selected_templates,
 selected_gates, is_active, created_by, created_at, updated_by, updated_at)
VALUES
('Retail_Customer_Segmentation_CLV',
 'RFM segmentation, lifetime value prediction, cohort analysis — customer PII, GDPR compliance scan.',
 'GitHub',
 '<your_github_username>',
 '<your_github_token>',
 '<your_repo_name>',
 'main',
 NULL,
 ARRAY('retail_nb_customer_segmentation_clv.ipynb'),
 '2026-05-14T06:30:00.000+00:00',
 'Daily',
 '2026-05-14T00:00:00.000+00:00',
 '2026-06-30T23:59:59.000+00:00',
 'Everyday',
 '-',
 '-',
 ARRAY(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,21,22,23,24),
 ARRAY(4,1,2),
 ARRAY(1,2,3,4,5,6,7),
 true,
 NULL,
 CURRENT_TIMESTAMP(),
 NULL,
 CURRENT_TIMESTAMP());


-- R3: Pricing Optimization Engine
INSERT INTO `eagleeye-uc`.code_inspector_metadata.scan_configs_1
(project_name, project_desc, source_type, user_name, token, repository_name, branch_name,
 folders, file_names, schedule_time, schedule_frequency, start_date, end_date,
 repeat_frequency, start_time, end_time, selected_rules, selected_templates,
 selected_gates, is_active, created_by, created_at, updated_by, updated_at)
VALUES
('Retail_Pricing_Optimization',
 'Dynamic pricing engine — competitive analysis, markdown optimization, price elasticity. Security + quality scan.',
 'GitHub',
 '<your_github_username>',
 '<your_github_token>',
 '<your_repo_name>',
 'main',
 NULL,
 ARRAY('retail_nb_pricing_optimization.ipynb'),
 '2026-05-14T07:00:00.000+00:00',
 'Daily',
 '2026-05-14T00:00:00.000+00:00',
 '2026-06-30T23:59:59.000+00:00',
 'Everyday',
 '-',
 '-',
 ARRAY(1,2,3,4,5,6,7,8,9,10,11,12,13,17,21,22,23,24),
 ARRAY(4,1,3),
 ARRAY(1,2,3,4,7),
 true,
 NULL,
 CURRENT_TIMESTAMP(),
 NULL,
 CURRENT_TIMESTAMP());


-- R4: Supply Chain & Logistics
INSERT INTO `eagleeye-uc`.code_inspector_metadata.scan_configs_1
(project_name, project_desc, source_type, user_name, token, repository_name, branch_name,
 folders, file_names, schedule_time, schedule_frequency, start_date, end_date,
 repeat_frequency, start_time, end_time, selected_rules, selected_templates,
 selected_gates, is_active, created_by, created_at, updated_by, updated_at)
VALUES
('Retail_Supply_Chain_Logistics',
 'Warehouse routing, supplier lead-time, last-mile delivery optimization. Full compliance + performance scan.',
 'GitHub',
 '<your_github_username>',
 '<your_github_token>',
 '<your_repo_name>',
 'main',
 NULL,
 ARRAY('retail_nb_supply_chain_logistics.ipynb'),
 '2026-05-14T07:30:00.000+00:00',
 'Daily',
 '2026-05-14T00:00:00.000+00:00',
 '2026-06-30T23:59:59.000+00:00',
 'Everyday',
 '-',
 '-',
 ARRAY(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,21,22,23,24),
 ARRAY(4,1,2,3),
 ARRAY(1,2,3,4,5,7),
 true,
 NULL,
 CURRENT_TIMESTAMP(),
 NULL,
 CURRENT_TIMESTAMP());


-- ═════════════════════════════════════════════════════════════════════
--  ENERGY DOMAIN (4 notebooks)
-- ═════════════════════════════════════════════════════════════════════

-- E1: Transformer Health Monitoring
INSERT INTO `eagleeye-uc`.code_inspector_metadata.scan_configs_1
(project_name, project_desc, source_type, user_name, token, repository_name, branch_name,
 folders, file_names, schedule_time, schedule_frequency, start_date, end_date,
 repeat_frequency, start_time, end_time, selected_rules, selected_templates,
 selected_gates, is_active, created_by, created_at, updated_by, updated_at)
VALUES
('Energy_Transformer_Health',
 'DGA analysis, thermal imaging, failure probability — SCADA credentials, operator PII, critical infrastructure security scan.',
 'GitHub',
 '<your_github_username>',
 '<your_github_token>',
 '<your_repo_name>',
 'main',
 NULL,
 ARRAY('energy_nb_transformer_health.ipynb'),
 '2026-05-14T08:00:00.000+00:00',
 'Daily',
 '2026-05-14T00:00:00.000+00:00',
 '2026-06-30T23:59:59.000+00:00',
 'Everyday',
 '-',
 '-',
 ARRAY(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,21,22,23,24),
 ARRAY(4,1,3),
 ARRAY(1,2,3,4,5,7),
 true,
 NULL,
 CURRENT_TIMESTAMP(),
 NULL,
 CURRENT_TIMESTAMP());


-- E2: Renewable Generation Forecasting
INSERT INTO `eagleeye-uc`.code_inspector_metadata.scan_configs_1
(project_name, project_desc, source_type, user_name, token, repository_name, branch_name,
 folders, file_names, schedule_time, schedule_frequency, start_date, end_date,
 repeat_frequency, start_time, end_time, selected_rules, selected_templates,
 selected_gates, is_active, created_by, created_at, updated_by, updated_at)
VALUES
('Energy_Renewable_Forecast',
 'Solar/wind generation prediction — weather model integration, operator PII, security + performance scan.',
 'GitHub',
 '<your_github_username>',
 '<your_github_token>',
 '<your_repo_name>',
 'main',
 NULL,
 ARRAY('energy_nb_renewable_forecast.ipynb'),
 '2026-05-14T08:30:00.000+00:00',
 'Daily',
 '2026-05-14T00:00:00.000+00:00',
 '2026-06-30T23:59:59.000+00:00',
 'Everyday',
 '-',
 '-',
 ARRAY(1,2,3,4,5,6,7,8,9,10,11,12,13,17,21,22,23,24),
 ARRAY(4,1,2),
 ARRAY(1,2,3,4,7),
 true,
 NULL,
 CURRENT_TIMESTAMP(),
 NULL,
 CURRENT_TIMESTAMP());


-- E3: Outage Prediction & Response
INSERT INTO `eagleeye-uc`.code_inspector_metadata.scan_configs_1
(project_name, project_desc, source_type, user_name, token, repository_name, branch_name,
 folders, file_names, schedule_time, schedule_frequency, start_date, end_date,
 repeat_frequency, start_time, end_time, selected_rules, selected_templates,
 selected_gates, is_active, created_by, created_at, updated_by, updated_at)
VALUES
('Energy_Outage_Prediction',
 'Outage risk from weather/load/vegetation — crew dispatch optimization, operator PII, full compliance scan.',
 'GitHub',
 '<your_github_username>',
 '<your_github_token>',
 '<your_repo_name>',
 'main',
 NULL,
 ARRAY('energy_nb_outage_prediction.ipynb'),
 '2026-05-14T09:00:00.000+00:00',
 'Daily',
 '2026-05-14T00:00:00.000+00:00',
 '2026-06-30T23:59:59.000+00:00',
 'Everyday',
 '-',
 '-',
 ARRAY(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,21,22,23,24),
 ARRAY(4,1,3),
 ARRAY(1,2,3,4,5,6,7),
 true,
 NULL,
 CURRENT_TIMESTAMP(),
 NULL,
 CURRENT_TIMESTAMP());


-- E4: Meter Data Analytics & Theft Detection
INSERT INTO `eagleeye-uc`.code_inspector_metadata.scan_configs_1
(project_name, project_desc, source_type, user_name, token, repository_name, branch_name,
 folders, file_names, schedule_time, schedule_frequency, start_date, end_date,
 repeat_frequency, start_time, end_time, selected_rules, selected_templates,
 selected_gates, is_active, created_by, created_at, updated_by, updated_at)
VALUES
('Energy_Meter_Theft_Detection',
 'AMI smart meter analytics — energy theft detection, non-technical losses, customer PII, revenue protection scan.',
 'GitHub',
 '<your_github_username>',
 '<your_github_token>',
 '<your_repo_name>',
 'main',
 NULL,
 ARRAY('energy_nb_meter_theft_detection.ipynb'),
 '2026-05-14T09:30:00.000+00:00',
 'Daily',
 '2026-05-14T00:00:00.000+00:00',
 '2026-06-30T23:59:59.000+00:00',
 'Everyday',
 '-',
 '-',
 ARRAY(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,21,22,23,24),
 ARRAY(4,1,2),
 ARRAY(1,2,3,4,5,7),
 true,
 NULL,
 CURRENT_TIMESTAMP(),
 NULL,
 CURRENT_TIMESTAMP());


-- ═════════════════════════════════════════════════════════════════════
--  TELECOM DOMAIN (4 notebooks)
-- ═════════════════════════════════════════════════════════════════════

-- T1: Network Quality & Cell Tower Analytics
INSERT INTO `eagleeye-uc`.code_inspector_metadata.scan_configs_1
(project_name, project_desc, source_type, user_name, token, repository_name, branch_name,
 folders, file_names, schedule_time, schedule_frequency, start_date, end_date,
 repeat_frequency, start_time, end_time, selected_rules, selected_templates,
 selected_gates, is_active, created_by, created_at, updated_by, updated_at)
VALUES
('Telecom_Network_Quality',
 'Cell tower KPIs, congestion prediction, NQI scoring — engineer PII, infrastructure credentials, full scan.',
 'GitHub',
 '<your_github_username>',
 '<your_github_token>',
 '<your_repo_name>',
 'main',
 NULL,
 ARRAY('telecom_nb_network_quality.ipynb'),
 '2026-05-14T10:00:00.000+00:00',
 'Daily',
 '2026-05-14T00:00:00.000+00:00',
 '2026-06-30T23:59:59.000+00:00',
 'Everyday',
 '-',
 '-',
 ARRAY(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,21,22,23,24),
 ARRAY(4,1,3),
 ARRAY(1,2,3,4,5,7),
 true,
 NULL,
 CURRENT_TIMESTAMP(),
 NULL,
 CURRENT_TIMESTAMP());


-- T2: Billing & Revenue Assurance
INSERT INTO `eagleeye-uc`.code_inspector_metadata.scan_configs_1
(project_name, project_desc, source_type, user_name, token, repository_name, branch_name,
 folders, file_names, schedule_time, schedule_frequency, start_date, end_date,
 repeat_frequency, start_time, end_time, selected_rules, selected_templates,
 selected_gates, is_active, created_by, created_at, updated_by, updated_at)
VALUES
('Telecom_Billing_Revenue_Assurance',
 'CDR reconciliation, rating validation, revenue leakage detection — subscriber PII, payment data, SOX compliance.',
 'GitHub',
 '<your_github_username>',
 '<your_github_token>',
 '<your_repo_name>',
 'main',
 NULL,
 ARRAY('telecom_nb_billing_revenue_assurance.ipynb'),
 '2026-05-14T10:30:00.000+00:00',
 'Daily',
 '2026-05-14T00:00:00.000+00:00',
 '2026-06-30T23:59:59.000+00:00',
 'Everyday',
 '-',
 '-',
 ARRAY(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,21,22,23,24),
 ARRAY(4,1,2,8),
 ARRAY(1,2,3,4,5,6,7),
 true,
 NULL,
 CURRENT_TIMESTAMP(),
 NULL,
 CURRENT_TIMESTAMP());


-- T3: Fraud Detection (SIM Swap & Subscription Fraud)
INSERT INTO `eagleeye-uc`.code_inspector_metadata.scan_configs_1
(project_name, project_desc, source_type, user_name, token, repository_name, branch_name,
 folders, file_names, schedule_time, schedule_frequency, start_date, end_date,
 repeat_frequency, start_time, end_time, selected_rules, selected_templates,
 selected_gates, is_active, created_by, created_at, updated_by, updated_at)
VALUES
('Telecom_Fraud_Detection',
 'SIM swap, IRSF, Wangiri, subscription fraud detection — subscriber PII, financial data, GDPR + security scan.',
 'GitHub',
 '<your_github_username>',
 '<your_github_token>',
 '<your_repo_name>',
 'main',
 NULL,
 ARRAY('telecom_nb_fraud_detection.ipynb'),
 '2026-05-14T11:00:00.000+00:00',
 'Daily',
 '2026-05-14T00:00:00.000+00:00',
 '2026-06-30T23:59:59.000+00:00',
 'Everyday',
 '-',
 '-',
 ARRAY(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,21,22,23,24),
 ARRAY(4,1,2),
 ARRAY(1,2,3,4,5,7),
 true,
 NULL,
 CURRENT_TIMESTAMP(),
 NULL,
 CURRENT_TIMESTAMP());


-- T4: Customer 360 & Personalization Engine
INSERT INTO `eagleeye-uc`.code_inspector_metadata.scan_configs_1
(project_name, project_desc, source_type, user_name, token, repository_name, branch_name,
 folders, file_names, schedule_time, schedule_frequency, start_date, end_date,
 repeat_frequency, start_time, end_time, selected_rules, selected_templates,
 selected_gates, is_active, created_by, created_at, updated_by, updated_at)
VALUES
('Telecom_Customer_360',
 'Unified customer view, next-best-action, real-time offer personalization — full PII, GDPR, security + quality scan.',
 'GitHub',
 '<your_github_username>',
 '<your_github_token>',
 '<your_repo_name>',
 'main',
 NULL,
 ARRAY('telecom_nb_customer_360.ipynb'),
 '2026-05-14T11:30:00.000+00:00',
 'Daily',
 '2026-05-14T00:00:00.000+00:00',
 '2026-06-30T23:59:59.000+00:00',
 'Everyday',
 '-',
 '-',
 ARRAY(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,21,22,23,24),
 ARRAY(4,1,2),
 ARRAY(1,2,3,4,5,6,7),
 true,
 NULL,
 CURRENT_TIMESTAMP(),
 NULL,
 CURRENT_TIMESTAMP());


-- ═════════════════════════════════════════════════════════════════════
-- VERIFIED PATTERN MATRIX — ALL 12 NOTEBOOKS
-- ═════════════════════════════════════════════════════════════════════
--
-- ┌─────────────────────────┬────────┬────────┬────────┬────────┬────────┬────────┬────────┬────────┬────────┬────────┬────────┬────────┐
-- │ Pattern                 │ R1-ETL │ R2-CLV │ R3-Prc │ R4-SCM │ E1-Xfm │ E2-Ren │ E3-Out │ E4-Mtr │ T1-Net │ T2-Bil │ T3-Frd │ T4-360 │
-- ├─────────────────────────┼────────┼────────┼────────┼────────┼────────┼────────┼────────┼────────┼────────┼────────┼────────┼────────┤
-- │ AWS Key (AKIA...)       │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │
-- │ API Key (api_key=...)   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │
-- │ DB Conn (postgres://..  │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │
-- │ JWT (eyJ...)            │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │
-- │ Password (password=...) │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │
-- │ Bearer Token            │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │
-- │ Email (user@domain)     │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │
-- │ Phone (+1-555-xxx)      │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │
-- │ SSN (123-45-6789)       │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │
-- │ Credit Card (4532...)   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │
-- │ Connection String       │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │
-- │ SQL Injection (f"SEL")  │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │
-- │ collect()               │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │
-- │ Hardcoded Paths         │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │
-- │ Code Duplication        │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │
-- │ Magic Numbers           │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │
-- │ Dead Variables          │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │
-- │ Bad Naming (a,b,c)      │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │
-- │ Deep Nesting            │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │
-- │ Function Length >10 pms │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │
-- │ Missing Docstrings      │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │
-- │ Unused Imports           │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │   ✅   │
-- ├─────────────────────────┼────────┼────────┼────────┼────────┼────────┼────────┼────────┼────────┼────────┼────────┼────────┼────────┤
-- │ EST TOTAL ISSUES        │  46+   │  48+   │  48+   │  50+   │  49+   │  55+   │  48+   │  55+   │  47+   │  49+   │  46+   │  47+   │
-- │ EXPECTED GRADE          │   F    │   F    │   F    │   F    │   F    │   F    │   F    │   F    │   F    │   F    │   F    │   F    │
-- │ ALL GATES               │  FAIL  │  FAIL  │  FAIL  │  FAIL  │  FAIL  │  FAIL  │  FAIL  │  FAIL  │  FAIL  │  FAIL  │  FAIL  │  FAIL  │
-- └─────────────────────────┴────────┴────────┴────────┴────────┴────────┴────────┴────────┴────────┴────────┴────────┴────────┴────────┘
