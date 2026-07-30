/*
===============================================================================
Project     : Energy Analytics Platform
Module      : Executive Energy Dashboard
Description : Executive KPI dashboard for Energy Analytics Platform.
Author      : Celebal Technologies
===============================================================================
*/

USE CATALOG eagleeyelakebase_uc;
USE SCHEMA energy;

-- ============================================================================
-- Dashboard 1 : Plant Generation KPI
-- ============================================================================

SELECT
    ppm.plant_name,
    SUM(pg.energy_generated_mwh) AS total_generation,
    ROUND(AVG(pg.efficiency_percentage),2) AS average_efficiency
FROM power_generation pg
INNER JOIN power_plant_master ppm
ON pg.plant_id = ppm.plant_id
GROUP BY
    ppm.plant_name
ORDER BY total_generation DESC;

-- ============================================================================
-- Dashboard 2 : Fuel Inventory Value
-- ============================================================================

SELECT
    fuel_type,
    SUM(quantity_available) AS stock_quantity,
    ROUND(SUM(quantity_available * unit_cost),2) AS inventory_value
FROM fuel_inventory
GROUP BY fuel_type
ORDER BY inventory_value DESC;

-- ============================================================================
-- Dashboard 3 : Consumer Revenue
-- ============================================================================

SELECT
    consumer_type,
    COUNT(*) AS consumers,
    SUM(total_amount) AS revenue
FROM consumer_billing
GROUP BY consumer_type
ORDER BY revenue DESC;

-- ============================================================================
-- Dashboard 4 : Revenue by Plant
-- ============================================================================

SELECT
    plant_id,
    SUM(revenue_amount) AS total_revenue
FROM revenue_fact
GROUP BY plant_id
ORDER BY total_revenue DESC;

-- ============================================================================
-- Dashboard 5 : Monthly Revenue Trend
-- ============================================================================

SELECT
    YEAR(revenue_date) AS revenue_year,
    MONTH(revenue_date) AS revenue_month,
    SUM(revenue_amount) AS monthly_revenue
FROM revenue_fact
GROUP BY
    YEAR(revenue_date),
    MONTH(revenue_date)
ORDER BY
    revenue_year,
    revenue_month;

-- ============================================================================
-- Dashboard 6 : Top Revenue Plants
-- ============================================================================

SELECT
    plant_id,
    SUM(revenue_amount) AS total_revenue,
    DENSE_RANK() OVER(
        ORDER BY SUM(revenue_amount) DESC
    ) AS revenue_rank
FROM revenue_fact
GROUP BY plant_id;

-- ============================================================================
-- Dashboard 7 : Executive KPI
-- ============================================================================

SELECT
    (SELECT COUNT(*) FROM power_plant_master) AS total_plants,
    (SELECT COUNT(*) FROM fuel_inventory) AS inventory_records,
    (SELECT SUM(energy_generated_mwh)
        FROM power_generation) AS total_generation,
    (SELECT SUM(total_amount)
        FROM consumer_billing) AS billing_revenue,
    (SELECT SUM(revenue_amount)
        FROM revenue_fact) AS total_revenue;
        