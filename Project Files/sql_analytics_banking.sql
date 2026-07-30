/* =============================================================================
   Gold Layer Build - Banking Domain (Eagle Eye Banking)
   =============================================================================
   Consumes the Silver tables built in silver_layer_banking.py and produces
   3 business-facing Gold tables (Databricks / Delta Lake SQL syntax).

   Silver sources used:
       banking_silver.customers
       banking_silver.accounts
       banking_silver.transactions
       banking_silver.loans
       banking_silver.cards
       banking_silver.compliance_alerts

   Gold tables produced:
       banking_gold.customer_360
       banking_gold.daily_transaction_summary
       banking_gold.loan_portfolio_risk_summary
   ============================================================================= */

CREATE DATABASE IF NOT EXISTS banking_gold;


/* -----------------------------------------------------------------------------
   1. banking_gold.customer_360
   -----------------------------------------------------------------------------
   One row per customer: profile + aggregated account, card, loan and
   compliance-alert exposure. Powers CRM/relationship-manager dashboards
   and customer-level risk views.
   ----------------------------------------------------------------------------- */
CREATE OR REPLACE TABLE banking_gold.customer_360
USING DELTA
PARTITIONED BY (risk_category)
AS
WITH account_agg AS (
    SELECT
        customer_id,
        COUNT(*)                                   AS total_accounts,
        SUM(CASE WHEN account_status = 'ACTIVE' THEN 1 ELSE 0 END)   AS active_accounts,
        SUM(CASE WHEN dormant_flag THEN 1 ELSE 0 END)                AS dormant_accounts,
        SUM(current_balance)                       AS total_current_balance,
        SUM(available_balance)                     AS total_available_balance
    FROM banking_silver.accounts
    GROUP BY customer_id
),
card_agg AS (
    SELECT
        customer_id,
        COUNT(*)                                   AS total_cards,
        SUM(CASE WHEN is_active_flag THEN 1 ELSE 0 END)              AS active_cards,
        SUM(credit_limit)                          AS total_credit_limit,
        SUM(available_credit_limit)                AS total_available_credit,
        ROUND(AVG(credit_utilization_ratio), 4)    AS avg_credit_utilization_ratio
    FROM banking_silver.cards
    GROUP BY customer_id
),
loan_agg AS (
    SELECT
        customer_id,
        COUNT(*)                                   AS total_loans,
        SUM(CASE WHEN loan_status = 'DISBURSED' THEN 1 ELSE 0 END)   AS active_loans,
        SUM(CASE WHEN is_defaulted_flag THEN 1 ELSE 0 END)           AS defaulted_loans,
        SUM(sanctioned_amount)                     AS total_sanctioned_amount,
        SUM(outstanding_amount)                    AS total_outstanding_amount,
        MAX(credit_score)                          AS latest_credit_score
    FROM banking_silver.loans
    GROUP BY customer_id
),
alert_agg AS (
    SELECT
        customer_id,
        COUNT(*)                                          AS total_compliance_alerts,
        SUM(CASE WHEN is_open_flag THEN 1 ELSE 0 END)      AS open_compliance_alerts,
        SUM(CASE WHEN is_high_severity_flag THEN 1 ELSE 0 END) AS high_severity_alerts,
        SUM(CASE WHEN sar_filed_flag THEN 1 ELSE 0 END)    AS sar_filed_count,
        MAX(risk_score)                                    AS max_risk_score
    FROM banking_silver.compliance_alerts
    WHERE customer_id IS NOT NULL
    GROUP BY customer_id
)
SELECT
    c.customer_id,
    c.full_name,
    c.customer_type,
    c.age_years,
    c.kyc_status,
    c.kyc_verified_flag,
    c.risk_category,
    c.high_risk_customer_flag,

    COALESCE(a.total_accounts, 0)               AS total_accounts,
    COALESCE(a.active_accounts, 0)              AS active_accounts,
    COALESCE(a.dormant_accounts, 0)             AS dormant_accounts,
    COALESCE(a.total_current_balance, 0)        AS total_current_balance,
    COALESCE(a.total_available_balance, 0)      AS total_available_balance,

    COALESCE(cc.total_cards, 0)                 AS total_cards,
    COALESCE(cc.active_cards, 0)                AS active_cards,
    COALESCE(cc.total_credit_limit, 0)          AS total_credit_limit,
    COALESCE(cc.total_available_credit, 0)      AS total_available_credit,
    cc.avg_credit_utilization_ratio,

    COALESCE(l.total_loans, 0)                  AS total_loans,
    COALESCE(l.active_loans, 0)                 AS active_loans,
    COALESCE(l.defaulted_loans, 0)              AS defaulted_loans,
    COALESCE(l.total_sanctioned_amount, 0)      AS total_sanctioned_amount,
    COALESCE(l.total_outstanding_amount, 0)     AS total_outstanding_amount,
    l.latest_credit_score,

    COALESCE(al.total_compliance_alerts, 0)     AS total_compliance_alerts,
    COALESCE(al.open_compliance_alerts, 0)      AS open_compliance_alerts,
    COALESCE(al.high_severity_alerts, 0)        AS high_severity_alerts,
    COALESCE(al.sar_filed_count, 0)             AS sar_filed_count,
    al.max_risk_score,

    -- overall relationship value tier used for segmentation
    CASE
        WHEN COALESCE(a.total_current_balance, 0) + COALESCE(l.total_outstanding_amount, 0) >= 5000000 THEN 'PLATINUM'
        WHEN COALESCE(a.total_current_balance, 0) + COALESCE(l.total_outstanding_amount, 0) >= 1000000 THEN 'GOLD'
        WHEN COALESCE(a.total_current_balance, 0) + COALESCE(l.total_outstanding_amount, 0) >= 100000  THEN 'SILVER'
        ELSE 'STANDARD'
    END                                          AS customer_value_tier,

    CURRENT_TIMESTAMP()                          AS gold_load_ts
FROM banking_silver.customers c
LEFT JOIN account_agg a  ON c.customer_id = a.customer_id
LEFT JOIN card_agg   cc  ON c.customer_id = cc.customer_id
LEFT JOIN loan_agg   l   ON c.customer_id = l.customer_id
LEFT JOIN alert_agg  al  ON c.customer_id = al.customer_id;


/* -----------------------------------------------------------------------------
   2. banking_gold.daily_transaction_summary
   -----------------------------------------------------------------------------
   Daily transaction volumes/values sliced by transaction type and channel.
   Powers ops dashboards, channel-performance tracking and anomaly detection
   baselines.
   ----------------------------------------------------------------------------- */
CREATE OR REPLACE TABLE banking_gold.daily_transaction_summary
USING DELTA
PARTITIONED BY (transaction_date)
AS
SELECT
    transaction_date,
    transaction_type,
    transaction_channel,
    transaction_currency,

    COUNT(*)                                                       AS total_transaction_count,
    SUM(CASE WHEN is_successful THEN 1 ELSE 0 END)                 AS successful_transaction_count,
    SUM(CASE WHEN transaction_status = 'FAILED' THEN 1 ELSE 0 END) AS failed_transaction_count,
    SUM(CASE WHEN transaction_status = 'REVERSED' THEN 1 ELSE 0 END) AS reversed_transaction_count,

    SUM(CASE WHEN debit_credit_indicator = 'DEBIT' THEN transaction_amount ELSE 0 END)  AS total_debit_amount,
    SUM(CASE WHEN debit_credit_indicator = 'CREDIT' THEN transaction_amount ELSE 0 END) AS total_credit_amount,
    SUM(transaction_amount)                                        AS total_transaction_value,
    ROUND(AVG(transaction_amount), 2)                              AS avg_transaction_value,
    MAX(transaction_amount)                                        AS max_transaction_value,

    SUM(CASE WHEN high_value_txn_flag THEN 1 ELSE 0 END)           AS high_value_transaction_count,
    COUNT(DISTINCT account_id)                                     AS distinct_accounts_active,
    COUNT(DISTINCT customer_id)                                    AS distinct_customers_active,

    CURRENT_TIMESTAMP()                                            AS gold_load_ts
FROM banking_silver.transactions
GROUP BY
    transaction_date,
    transaction_type,
    transaction_channel,
    transaction_currency;


/* -----------------------------------------------------------------------------
   3. banking_gold.loan_portfolio_risk_summary
   -----------------------------------------------------------------------------
   Loan book summarized by loan type / status / credit-score band.
   Powers credit-risk and portfolio-quality reporting.
   ----------------------------------------------------------------------------- */
CREATE OR REPLACE TABLE banking_gold.loan_portfolio_risk_summary
USING DELTA
PARTITIONED BY (loan_type)
AS
SELECT
    loan_type,
    loan_status,
    credit_score_band,

    COUNT(*)                                                   AS total_loans,
    COUNT(DISTINCT customer_id)                                AS distinct_borrowers,

    SUM(sanctioned_amount)                                     AS total_sanctioned_amount,
    SUM(outstanding_amount)                                    AS total_outstanding_amount,
    ROUND(AVG(outstanding_to_sanctioned_ratio), 4)             AS avg_outstanding_ratio,
    ROUND(AVG(interest_rate), 2)                               AS avg_interest_rate,
    ROUND(AVG(emi_amount), 2)                                  AS avg_emi_amount,
    ROUND(AVG(credit_score), 0)                                AS avg_credit_score,

    SUM(CASE WHEN is_overdue_flag THEN 1 ELSE 0 END)           AS overdue_loan_count,
    SUM(CASE WHEN is_defaulted_flag THEN 1 ELSE 0 END)         AS defaulted_loan_count,
    ROUND(
        SUM(CASE WHEN is_defaulted_flag THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
        4
    )                                                           AS default_rate,

    CURRENT_TIMESTAMP()                                        AS gold_load_ts
FROM banking_silver.loans
GROUP BY
    loan_type,
    loan_status,
    credit_score_band;
