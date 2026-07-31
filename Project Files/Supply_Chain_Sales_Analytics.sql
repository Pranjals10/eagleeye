/*
===============================================================================
Project         : Supply Chain Analytics Platform
Module          : Sales Analytics
Description     : Sales performance and revenue analysis
Author          : Celebal Technologies
===============================================================================
*/

USE CATALOG eagleeyelakebase_uc;

USE SCHEMA supply_chain_demo;

-- ============================================================================
-- 1. Sales Details
-- ============================================================================

SELECT
    sales_id,
    customer_order_id,
    product_id,
    sales_date,
    quantity_sold,
    sales_amount,
    profit_amount,
    region
FROM sales_fact
ORDER BY sales_date DESC;

-- ============================================================================
-- 2. Sales Summary
-- ============================================================================

SELECT
    COUNT(*) AS total_sales_transactions,
    SUM(sales_amount) AS total_sales,
    SUM(profit_amount) AS total_profit,
    AVG(sales_amount) AS average_sales
FROM sales_fact;

-- ============================================================================
-- 3. Regional Sales Performance
-- ============================================================================

SELECT
    region,
    COUNT(*) AS transactions,
    SUM(sales_amount) AS total_sales,
    SUM(profit_amount) AS total_profit
FROM sales_fact
GROUP BY region
ORDER BY total_sales DESC;

-- ============================================================================
-- 4. Product Performance
-- ============================================================================

SELECT
    product_id,
    SUM(quantity_sold) AS units_sold,
    SUM(sales_amount) AS revenue,
    SUM(profit_amount) AS profit
FROM sales_fact
GROUP BY product_id
ORDER BY revenue DESC;

-- ============================================================================
-- 5. Monthly Sales Trend
-- ============================================================================

SELECT
    YEAR(sales_date) AS sales_year,
    MONTH(sales_date) AS sales_month,
    SUM(sales_amount) AS total_sales,
    SUM(profit_amount) AS total_profit
FROM sales_fact
GROUP BY
    YEAR(sales_date),
    MONTH(sales_date)
ORDER BY
    sales_year,
    sales_month;

-- ============================================================================
-- 6. Highest Revenue Transactions
-- ============================================================================

SELECT
    sales_id,
    customer_order_id,
    sales_amount,
    profit_amount
FROM sales_fact
ORDER BY sales_amount DESC
LIMIT 10;

-- ============================================================================
-- 7. Profit Margin Analysis
-- ============================================================================

SELECT
    sales_id,
    sales_amount,
    profit_amount,
    ROUND((profit_amount / sales_amount) * 100,2) AS profit_margin_percentage
FROM sales_fact
WHERE sales_amount > 0
ORDER BY profit_margin_percentage DESC;