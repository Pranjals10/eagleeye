-- =============================================================================
-- build_gold_tables_3.sql
--
-- Retail_CPG - Silver -> Gold transformation (Databricks SQL / Unity Catalog).
--
-- Consumes the 6 Silver tables produced by build_silver_tables_3.py and builds
-- 3 Gold, business-facing, aggregated marts:
--
--   1. gold_sales_performance_daily     - daily sales KPIs by store/channel/category
--   2. gold_vendor_compliance_scorecard - vendor-level compliance & audit rollup
--   3. gold_store_operations_daily      - combined store ops + sales performance
--
-- Catalog / schema convention:
--   Silver : eagleeyelakebase_uc.retail_cpg_silver
--   Gold   : eagleeyelakebase_uc.retail_cpg_gold
--
-- No secrets, keys, or credentials are ever embedded in this file. Storage
-- and service credentials are configured once at the Unity Catalog external
-- location / storage credential level and referenced by the catalog itself,
-- never inline in a query.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS eagleeyelakebase_uc.retail_cpg_gold;

-- Row-level security: restricts gold-layer rows to the caller's own
-- business unit / region unless they hold the elevated analytics group.
-- Applied to both tables below that carry region-scoped financial and
-- vendor-risk data.
CREATE OR REPLACE FUNCTION eagleeyelakebase_uc.retail_cpg_gold.region_row_filter(region STRING)
RETURN
    IS_ACCOUNT_GROUP_MEMBER('retail_cpg_global_analytics')
    OR region = CURRENT_USER_DEFAULT_REGION();

-- -----------------------------------------------------------------------------
-- 1. gold_sales_performance_daily
--    Grain: transaction_date x store_id x sales_channel x category_name
--    Answers: "how is each store/channel/category performing, day over day?"
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE eagleeyelakebase_uc.retail_cpg_gold.gold_sales_performance_daily
COMMENT 'Daily sales KPIs by store, sales channel and product category'
PARTITIONED BY (transaction_date)
AS
WITH sales_txn AS (
    SELECT
        transaction_date,
        store_id,
        sales_channel,
        sku_id,
        customer_id,
        transaction_id,
        UPPER(transaction_type) AS transaction_type,
        quantity_sold,
        net_amount,
        discount_amount
    FROM eagleeyelakebase_uc.retail_cpg_silver.sales_transactions_silver
    -- Early partition filter: incremental daily rebuild window instead of a
    -- full historical scan. Pass a widget/parameter for the actual job run.
    WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), 400)
),
sales_with_product AS (
    SELECT /*+ BROADCAST(product) */
        sales_txn.transaction_date,
        sales_txn.store_id,
        sales_txn.sales_channel,
        product.category_name,
        product.brand_name,
        sales_txn.transaction_id,
        sales_txn.customer_id,
        sales_txn.transaction_type,
        sales_txn.quantity_sold,
        sales_txn.net_amount,
        sales_txn.discount_amount
    FROM sales_txn
    LEFT JOIN eagleeyelakebase_uc.retail_cpg_silver.products_silver AS product
           ON sales_txn.sku_id = product.sku_id
)
SELECT
    transaction_date,
    store_id,
    sales_channel,
    category_name,
    brand_name,
    COUNT(DISTINCT transaction_id) AS transaction_count,
    COUNT(DISTINCT customer_id) AS distinct_customers,
    SUM(CASE WHEN transaction_type = 'SALE' THEN quantity_sold ELSE 0 END) AS units_sold,
    SUM(CASE WHEN transaction_type = 'RETURN' THEN quantity_sold ELSE 0 END) AS units_returned,
    ROUND(
        SUM(
            CASE
                WHEN transaction_type = 'SALE' THEN net_amount
                WHEN transaction_type = 'RETURN' THEN -net_amount
                ELSE 0
            END
        ),
        2
    ) AS net_revenue,
    ROUND(SUM(discount_amount), 2) AS total_discount_given,
    ROUND(
        SUM(CASE WHEN transaction_type = 'SALE' THEN net_amount ELSE 0 END)
        / NULLIF(COUNT(DISTINCT CASE WHEN transaction_type = 'SALE' THEN transaction_id END), 0),
        2
    ) AS avg_order_value,
    SUM(net_amount) OVER (
        PARTITION BY store_id
        ORDER BY transaction_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_7day_store_revenue
FROM sales_with_product
GROUP BY
    transaction_date,
    store_id,
    sales_channel,
    category_name,
    brand_name;

OPTIMIZE eagleeyelakebase_uc.retail_cpg_gold.gold_sales_performance_daily
ZORDER BY (store_id, transaction_date);

-- -----------------------------------------------------------------------------
-- 2. gold_vendor_compliance_scorecard
--    Grain: vendor_id
--    Answers: "which vendors are compliant / at risk, and who needs follow-up?"
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE eagleeyelakebase_uc.retail_cpg_gold.gold_vendor_compliance_scorecard
COMMENT 'Vendor-level rollup of audits, compliance scores and corrective actions'
AS
WITH vendor_compliance AS (
    SELECT
        vendor_id,
        vendor_name,
        vendor_category,
        vendor_region,
        audit_id,
        UPPER(compliance_status) AS compliance_status,
        compliance_score,
        corrective_action_required,
        is_certificate_expired,
        audit_timestamp
    FROM eagleeyelakebase_uc.retail_cpg_silver.vendor_compliance_silver
)
SELECT
    vendor_id,
    ANY_VALUE(vendor_name) AS vendor_name,
    ANY_VALUE(vendor_category) AS vendor_category,
    ANY_VALUE(vendor_region) AS vendor_region,
    COUNT(DISTINCT audit_id) AS total_audits,
    ROUND(AVG(compliance_score), 2) AS avg_compliance_score,
    MIN(compliance_score) AS min_compliance_score,
    MAX(compliance_score) AS max_compliance_score,
    SUM(CASE WHEN compliance_status = 'COMPLIANT' THEN 1 ELSE 0 END) AS compliant_audit_count,
    SUM(CASE WHEN compliance_status = 'NON_COMPLIANT' THEN 1 ELSE 0 END) AS non_compliant_audit_count,
    SUM(CASE WHEN compliance_status = 'EXPIRED' THEN 1 ELSE 0 END) AS expired_audit_count,
    ROUND(
        100.0 * SUM(CASE WHEN compliance_status = 'COMPLIANT' THEN 1 ELSE 0 END) / COUNT(*),
        2
    ) AS compliant_pct,
    SUM(CASE WHEN corrective_action_required = TRUE THEN 1 ELSE 0 END) AS open_corrective_actions,
    SUM(CASE WHEN is_certificate_expired = TRUE THEN 1 ELSE 0 END) AS expired_certificate_count,
    MAX(audit_timestamp) AS last_audit_timestamp,
    CASE
        WHEN AVG(compliance_score) >= 90 THEN 'LOW_RISK'
        WHEN AVG(compliance_score) >= 75 THEN 'MEDIUM_RISK'
        ELSE 'HIGH_RISK'
    END AS vendor_risk_tier
FROM vendor_compliance
GROUP BY vendor_id;

ALTER TABLE eagleeyelakebase_uc.retail_cpg_gold.gold_vendor_compliance_scorecard
ADD CONSTRAINT pk_gold_vendor_compliance_scorecard PRIMARY KEY (vendor_id) NOT ENFORCED;

ALTER TABLE eagleeyelakebase_uc.retail_cpg_gold.gold_vendor_compliance_scorecard
SET ROW FILTER eagleeyelakebase_uc.retail_cpg_gold.region_row_filter ON (vendor_region);

OPTIMIZE eagleeyelakebase_uc.retail_cpg_gold.gold_vendor_compliance_scorecard
ZORDER BY (vendor_id);

-- -----------------------------------------------------------------------------
-- 3. gold_store_operations_daily
--    Grain: business_date x store_id
--    Answers: "how did each store perform operationally AND commercially,
--              on a given day?" (combines ops health with sales results)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE eagleeyelakebase_uc.retail_cpg_gold.gold_store_operations_daily
COMMENT 'Combined daily store operational health and sales performance'
PARTITIONED BY (business_date)
AS
WITH store_ops AS (
    SELECT
        business_date,
        store_id,
        employee_id_hash,
        store_region,
        UPPER(operation_type) AS operation_type,
        UPPER(operation_status) AS operation_status,
        store_compliance_score,
        cash_variance_amount,
        has_cash_variance
    FROM eagleeyelakebase_uc.retail_cpg_silver.store_operations_silver
),
ops_daily AS (
    SELECT
        business_date,
        store_id,
        ANY_VALUE(store_region) AS store_region,
        COUNT(*) AS total_operational_events,
        SUM(CASE WHEN operation_type = 'INCIDENT' THEN 1 ELSE 0 END) AS incident_count,
        SUM(CASE WHEN operation_status = 'FAILED' THEN 1 ELSE 0 END) AS failed_operation_count,
        ROUND(AVG(store_compliance_score), 2) AS avg_store_compliance_score,
        ROUND(SUM(cash_variance_amount), 2) AS total_cash_variance,
        SUM(CASE WHEN has_cash_variance THEN 1 ELSE 0 END) AS cash_variance_event_count
    FROM store_ops
    GROUP BY business_date, store_id
),
sales_daily AS (
    SELECT
        transaction_date,
        store_id,
        COUNT(DISTINCT transaction_id) AS transaction_count,
        ROUND(
            SUM(
                CASE
                    WHEN UPPER(transaction_type) = 'SALE' THEN net_amount
                    WHEN UPPER(transaction_type) = 'RETURN' THEN -net_amount
                    ELSE 0
                END
            ),
            2
        ) AS net_revenue
    FROM eagleeyelakebase_uc.retail_cpg_silver.sales_transactions_silver
    GROUP BY transaction_date, store_id
)
SELECT
    COALESCE(ops_daily.business_date, sales_daily.transaction_date) AS business_date,
    COALESCE(ops_daily.store_id, sales_daily.store_id) AS store_id,
    COALESCE(ops_daily.store_region, 'UNKNOWN') AS store_region,
    COALESCE(ops_daily.total_operational_events, 0) AS total_operational_events,
    COALESCE(ops_daily.incident_count, 0) AS incident_count,
    COALESCE(ops_daily.failed_operation_count, 0) AS failed_operation_count,
    ops_daily.avg_store_compliance_score,
    COALESCE(ops_daily.total_cash_variance, 0.0) AS total_cash_variance,
    COALESCE(ops_daily.cash_variance_event_count, 0) AS cash_variance_event_count,
    COALESCE(sales_daily.transaction_count, 0) AS transaction_count,
    COALESCE(sales_daily.net_revenue, 0.0) AS net_revenue,
    CASE
        WHEN COALESCE(ops_daily.incident_count, 0) > 0
             OR COALESCE(ops_daily.avg_store_compliance_score, 100) < 80 THEN 'NEEDS_ATTENTION'
        ELSE 'HEALTHY'
    END AS store_health_flag
FROM ops_daily
FULL OUTER JOIN sales_daily
    ON ops_daily.business_date IS NOT DISTINCT FROM sales_daily.transaction_date
   AND ops_daily.store_id IS NOT DISTINCT FROM sales_daily.store_id;

ALTER TABLE eagleeyelakebase_uc.retail_cpg_gold.gold_store_operations_daily
SET ROW FILTER eagleeyelakebase_uc.retail_cpg_gold.region_row_filter ON (store_region);

OPTIMIZE eagleeyelakebase_uc.retail_cpg_gold.gold_store_operations_daily
ZORDER BY (store_id, business_date);
