/*
===============================================================================
Project     : Energy Analytics Platform
Module      : Consumer Billing Analytics
Description : Consumer billing, payment and revenue analysis.
Author      : Celebal Technologies
===============================================================================
*/

USE CATALOG eagleeyelakebase_uc;
USE SCHEMA energy;

-- ============================================================================
-- 1. Consumer Billing Overview
-- ============================================================================

SELECT
    billing_id,
    consumer_id,
    billing_month,
    units_consumed,
    total_amount,
    payment_status
FROM consumer_billing
ORDER BY billing_month DESC;

-- ============================================================================
-- 2. Monthly Billing Summary
-- ============================================================================

SELECT
    billing_month,
    COUNT(*) AS total_bills,
    SUM(units_consumed) AS total_units,
    SUM(total_amount) AS total_revenue
FROM consumer_billing
GROUP BY billing_month
ORDER BY billing_month;

-- ============================================================================
-- 3. Consumer Type Analysis
-- ============================================================================

SELECT
    consumer_type,
    COUNT(*) AS consumer_count,
    SUM(total_amount) AS revenue,
    AVG(units_consumed) AS average_units
FROM consumer_billing
GROUP BY consumer_type
ORDER BY revenue DESC;

-- ============================================================================
-- 4. Payment Status Analysis
-- ============================================================================

SELECT
    payment_status,
    COUNT(*) AS bills,
    SUM(total_amount) AS amount
FROM consumer_billing
GROUP BY payment_status
ORDER BY amount DESC;

-- ============================================================================
-- 5. Highest Bills
-- ============================================================================

SELECT
    consumer_id,
    consumer_type,
    units_consumed,
    total_amount
FROM consumer_billing
ORDER BY total_amount DESC
LIMIT 10;

-- ============================================================================
-- 6. Pending Payments
-- ============================================================================

SELECT
    billing_id,
    consumer_id,
    billing_month,
    total_amount
FROM consumer_billing
WHERE payment_status = 'Pending'
ORDER BY total_amount DESC;

-- ============================================================================
-- 7. Billing Ranking
-- ============================================================================

SELECT
    consumer_id,
    billing_month,
    total_amount,
    DENSE_RANK() OVER(
        ORDER BY total_amount DESC
    ) AS bill_rank
FROM consumer_billing;

-- ============================================================================
-- 8. Consumer Billing KPI
-- ============================================================================

SELECT
    COUNT(*) AS total_bills,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS average_bill_amount,
    MAX(total_amount) AS highest_bill,
    MIN(total_amount) AS lowest_bill
FROM consumer_billing;