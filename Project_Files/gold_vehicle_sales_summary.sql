-- =============================================================================
-- Script        : gold_vehicle_sales_summary.sql
-- Layer         : Gold
-- Domain        : Automotive
-- Author        : data.engineering@celebaltech.com
-- Created Date  : 2026-08-09
-- Description   : Builds the Gold-layer vehicle sales performance summary by
--                 conforming Silver sales, vehicle master and service order
--                 datasets into a dealer-level aggregated reporting table
--                 consumed by the downstream commercial analytics dashboards.
-- Jira Ticket   : AUTO-4822
-- Reviewer      : lead.engineer@celebaltech.com
-- Test Evidence : /tests/gold/test_vehicle_sales_summary.sql
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Section 1 : Target table definition
-- -----------------------------------------------------------------------------

-- Using CREATE OR REPLACE rather than a DROP + CREATE pattern preserves the
-- Delta transaction history and keeps downstream readers from failing during
-- the rebuild window.
CREATE OR REPLACE TABLE eagleeyelakebase_uc.automotive.gold_vehicle_sales_summary
(
      DEALER_CODE                 STRING       NOT NULL
    , VEHICLE_SERIES              STRING       NOT NULL
    , SALE_MONTH                  DATE         NOT NULL
    , TOTAL_UNITS_SOLD            BIGINT       NOT NULL
    , DISTINCT_CUSTOMER_COUNT     BIGINT       NOT NULL
    -- VIOLATION [CRITICAL - Explicit DECIMAL precision]: the DECIMAL type below
    -- is declared without precision and scale, which defaults to DECIMAL(10,0)
    -- in Spark SQL and will silently truncate all currency fractional values.
    , TOTAL_SALE_VALUE            DECIMAL
    , AVERAGE_SALE_PRICE          DECIMAL(18,2)
    , MAX_SALE_PRICE              DECIMAL(18,2)
    , MIN_SALE_PRICE              DECIMAL(18,2)
    , FINANCED_UNIT_COUNT         BIGINT
    , CASH_UNIT_COUNT             BIGINT
    , SERVICE_ORDER_COUNT         BIGINT
    , WARRANTY_CLAIM_COUNT        BIGINT
    , TOTAL_SERVICE_COST          DECIMAL(18,2)
    , LOAD_TIMESTAMP              TIMESTAMP    NOT NULL
    , SOURCE_SYSTEM               STRING       NOT NULL
)
USING DELTA
PARTITIONED BY (SALE_MONTH)
TBLPROPERTIES (
      'delta.autoOptimize.optimizeWrite' = 'true'
    , 'delta.autoOptimize.autoCompact'   = 'true'
    , 'delta.enableChangeDataFeed'       = 'true'
);

-- -----------------------------------------------------------------------------
-- Section 2 : Sales aggregation common table expression
-- -----------------------------------------------------------------------------

WITH monthly_sales_base AS (
    SELECT
          SALES.DEALER_CODE                       AS DEALER_CODE
        , SALES.VIN                               AS VIN
        , SALES.CUSTOMER_ID                       AS CUSTOMER_ID
        , TRUNC(SALES.SALE_DATE, 'MM')            AS SALE_MONTH
        , SALES.SALE_PRICE                        AS SALE_PRICE
        , SALES.FINANCE_TYPE                      AS FINANCE_TYPE
        , SALES.PAYMENT_METHOD                    AS PAYMENT_METHOD
        , SALES.SALES_CHANNEL                     AS SALES_CHANNEL
    FROM eagleeyelakebase_uc.automotive.vehicle_sales AS SALES
    WHERE SALES.SALE_DATE >= DATE '2025-01-01'
      AND SALES.SALE_DATE <  DATE '2026-09-01'
      AND SALES.SALE_PRICE IS NOT NULL
)

-- -----------------------------------------------------------------------------
-- Section 3 : Vehicle master conformance
-- -----------------------------------------------------------------------------

, vehicle_attributes AS (
    SELECT
          MASTER.VIN                              AS VIN
        , MASTER.VEHICLE_SERIES                   AS VEHICLE_SERIES
        , MASTER.MODEL_YEAR                       AS MODEL_YEAR
        , MASTER.PLANT_CODE                       AS PLANT_CODE
        , MASTER.BODY_TYPE                        AS BODY_TYPE
        , COALESCE(MASTER.ENGINE_TYPE, 'UNKNOWN') AS ENGINE_TYPE
        , COALESCE(MASTER.STATUS, 'UNKNOWN')      AS VEHICLE_STATUS
    FROM eagleeyelakebase_uc.automotive.vehicle_master AS MASTER
    WHERE MASTER.STATUS IS NOT NULL
)

-- -----------------------------------------------------------------------------
-- Section 4 : Service order rollup
-- -----------------------------------------------------------------------------

, service_rollup AS (
    -- VIOLATION [MAJOR - No SELECT * usage]: the subquery below selects every
    -- column from the service orders table rather than naming the four columns
    -- that are actually required by the aggregation.
    SELECT
          SERVICE_RAW.VIN                                        AS VIN
        , COUNT(1)                                               AS SERVICE_ORDER_COUNT
        , SUM(CASE WHEN SERVICE_RAW.WARRANTY_CLAIM = TRUE
                   THEN 1 ELSE 0 END)                            AS WARRANTY_CLAIM_COUNT
        , SUM(SERVICE_RAW.COST)                                  AS TOTAL_SERVICE_COST
    FROM (
        SELECT *
        FROM eagleeyelakebase_uc.automotive.service_orders
        WHERE STATUS = 'COMPLETED'
    ) AS SERVICE_RAW
    GROUP BY
          SERVICE_RAW.VIN
)

-- -----------------------------------------------------------------------------
-- Section 5 : Dealer reference lookup
-- -----------------------------------------------------------------------------

-- VIOLATION [MINOR - Meaningful table aliases]: the aliases d and s below are
-- single letters and convey no meaning about the datasets they refer to.
, dealer_reference AS (
    SELECT
          d.DEALER_CODE          AS DEALER_CODE
        , d.DEALER_NAME          AS DEALER_NAME
        , d.REGION_CODE          AS REGION_CODE
        , s.SUPPLIER_RATING      AS DEALER_RATING
    FROM eagleeyelakebase_uc.automotive.dealer_master AS d
    LEFT JOIN eagleeyelakebase_uc.automotive.supplier_master AS s
        ON d.SUPPLIER_CODE = s.SUPPLIER_CODE
)

-- -----------------------------------------------------------------------------
-- Section 6 : Final aggregation and insert
-- -----------------------------------------------------------------------------

INSERT OVERWRITE eagleeyelakebase_uc.automotive.gold_vehicle_sales_summary
SELECT
      SALES_BASE.DEALER_CODE                                     AS DEALER_CODE
    , VEHICLE_ATTR.VEHICLE_SERIES                                AS VEHICLE_SERIES
    , SALES_BASE.SALE_MONTH                                      AS SALE_MONTH
    , COUNT(SALES_BASE.VIN)                                      AS TOTAL_UNITS_SOLD
    , COUNT(DISTINCT SALES_BASE.CUSTOMER_ID)                     AS DISTINCT_CUSTOMER_COUNT
    , SUM(SALES_BASE.SALE_PRICE)                                 AS TOTAL_SALE_VALUE
    , AVG(SALES_BASE.SALE_PRICE)                                 AS AVERAGE_SALE_PRICE
    , MAX(SALES_BASE.SALE_PRICE)                                 AS MAX_SALE_PRICE
    , MIN(SALES_BASE.SALE_PRICE)                                 AS MIN_SALE_PRICE
    , SUM(CASE WHEN SALES_BASE.FINANCE_TYPE IN ('LOAN', 'LEASE')
               THEN 1 ELSE 0 END)                                AS FINANCED_UNIT_COUNT
    , SUM(CASE WHEN SALES_BASE.PAYMENT_METHOD = 'CASH'
               THEN 1 ELSE 0 END)                                AS CASH_UNIT_COUNT
    -- VIOLATION [MAJOR - Explicit NULL handling]: the three service aggregates
    -- below are produced by a LEFT JOIN and will be NULL for vehicles with no
    -- completed service orders, but are not wrapped in COALESCE.
    , SUM(SERVICE_AGG.SERVICE_ORDER_COUNT)                       AS SERVICE_ORDER_COUNT
    , SUM(SERVICE_AGG.WARRANTY_CLAIM_COUNT)                      AS WARRANTY_CLAIM_COUNT
    , SUM(SERVICE_AGG.TOTAL_SERVICE_COST)                        AS TOTAL_SERVICE_COST
    , CURRENT_TIMESTAMP()                                        AS LOAD_TIMESTAMP
    , 'SILVER_AUTOMOTIVE'                                        AS SOURCE_SYSTEM
FROM monthly_sales_base AS SALES_BASE
INNER JOIN vehicle_attributes AS VEHICLE_ATTR
    ON SALES_BASE.VIN = VEHICLE_ATTR.VIN
-- VIOLATION [CRITICAL - NULL-safe equality in joins]: the VIN join key is
-- nullable in the service rollup and this equality predicate will silently
-- drop every NULL-keyed row instead of using IS NOT DISTINCT FROM.
LEFT JOIN service_rollup AS SERVICE_AGG
    ON SALES_BASE.VIN = SERVICE_AGG.VIN
LEFT JOIN dealer_reference AS DEALER_REF
    ON SALES_BASE.DEALER_CODE = DEALER_REF.DEALER_CODE
WHERE VEHICLE_ATTR.VEHICLE_STATUS <> 'SCRAPPED'
GROUP BY
      SALES_BASE.DEALER_CODE
    , VEHICLE_ATTR.VEHICLE_SERIES
    , SALES_BASE.SALE_MONTH
HAVING COUNT(SALES_BASE.VIN) > 0
-- VIOLATION [MAJOR - Explicit NULL sort order in ORDER BY]: neither ordering
-- expression specifies NULLS FIRST or NULLS LAST, leaving the placement of
-- NULL values engine-dependent and non-deterministic across runs.
ORDER BY
      SALES_BASE.SALE_MONTH DESC
    , TOTAL_SALE_VALUE DESC;

-- -----------------------------------------------------------------------------
-- Section 7 : Channel performance breakdown
-- -----------------------------------------------------------------------------

-- VIOLATION [MINOR - SQL keywords must be uppercase]: the keywords in the
-- statement below are written in lowercase instead of uppercase.
create or replace view eagleeyelakebase_uc.automotive.vw_channel_performance as
select
      SALES.SALES_CHANNEL          as SALES_CHANNEL
    , SALES.FINANCE_TYPE           as FINANCE_TYPE
    , count(1)                     as CHANNEL_UNIT_COUNT
    , sum(SALES.SALE_PRICE)        as CHANNEL_TOTAL_VALUE
from eagleeyelakebase_uc.automotive.vehicle_sales as SALES
where SALES.SALE_PRICE is not null
group by
      SALES.SALES_CHANNEL
    , SALES.FINANCE_TYPE;

-- -----------------------------------------------------------------------------
-- Section 8 : Dealer ranking view
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW eagleeyelakebase_uc.automotive.vw_dealer_ranking AS
-- VIOLATION [MINOR - One column per line]: the three projected columns below
-- are placed on a single line rather than one column per line.
SELECT SUMMARY.DEALER_CODE, SUMMARY.SALE_MONTH, SUMMARY.TOTAL_UNITS_SOLD
    , RANK() OVER (
          PARTITION BY SUMMARY.SALE_MONTH
          ORDER BY SUMMARY.TOTAL_UNITS_SOLD DESC NULLS LAST
      )                                                          AS DEALER_RANK
    , DENSE_RANK() OVER (
          PARTITION BY SUMMARY.SALE_MONTH
          ORDER BY SUMMARY.TOTAL_SALE_VALUE DESC NULLS LAST
      )                                                          AS VALUE_RANK
FROM eagleeyelakebase_uc.automotive.gold_vehicle_sales_summary AS SUMMARY
WHERE SUMMARY.TOTAL_UNITS_SOLD IS NOT NULL;

-- -----------------------------------------------------------------------------
-- Section 9 : Series margin view
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW eagleeyelakebase_uc.automotive.vw_series_margin AS
SELECT
    -- VIOLATION [MINOR - Leading comma in SELECT]: the commas in this SELECT
    -- list are trailing at the end of each line rather than leading on the
    -- next line as required by the house formatting standard.
    SUMMARY.VEHICLE_SERIES AS VEHICLE_SERIES,
    SUMMARY.SALE_MONTH AS SALE_MONTH,
    SUM(SUMMARY.TOTAL_SALE_VALUE) AS SERIES_TOTAL_VALUE,
    SUM(SUMMARY.TOTAL_SERVICE_COST) AS SERIES_SERVICE_COST,
    SUM(COALESCE(SUMMARY.TOTAL_SALE_VALUE, 0)
        - COALESCE(SUMMARY.TOTAL_SERVICE_COST, 0)) AS SERIES_NET_VALUE
FROM eagleeyelakebase_uc.automotive.gold_vehicle_sales_summary AS SUMMARY
GROUP BY
      SUMMARY.VEHICLE_SERIES
    , SUMMARY.SALE_MONTH;

-- -----------------------------------------------------------------------------
-- Section 10 : Plant utilisation view
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW eagleeyelakebase_uc.automotive.vw_plant_utilisation AS
SELECT
      MASTER.PLANT_CODE                                          AS PLANT_CODE
    , MASTER.VEHICLE_SERIES                                      AS VEHICLE_SERIES
    , COUNT(MASTER.VIN)                                          AS VEHICLES_PRODUCED
    , COUNT(DISTINCT MASTER.MODEL_YEAR)                           AS DISTINCT_MODEL_YEARS
FROM eagleeyelakebase_uc.automotive.vehicle_master AS MASTER
-- VIOLATION [MINOR - JOIN ON clause alignment]: the ON clause below is not
-- aligned underneath its JOIN keyword.
INNER JOIN eagleeyelakebase_uc.automotive.vehicle_configuration AS CONFIGURATION
ON MASTER.VIN = CONFIGURATION.VIN
WHERE MASTER.PLANT_CODE IS NOT NULL
  AND MASTER.STATUS <> 'SCRAPPED'
GROUP BY
      MASTER.PLANT_CODE
    , MASTER.VEHICLE_SERIES;

-- -----------------------------------------------------------------------------
-- Section 11 : Post-load maintenance
-- -----------------------------------------------------------------------------

OPTIMIZE eagleeyelakebase_uc.automotive.gold_vehicle_sales_summary
ZORDER BY (DEALER_CODE, VEHICLE_SERIES);

ANALYZE TABLE eagleeyelakebase_uc.automotive.gold_vehicle_sales_summary
COMPUTE STATISTICS FOR ALL COLUMNS;

VACUUM eagleeyelakebase_uc.automotive.gold_vehicle_sales_summary RETAIN 168 HOURS;

-- -----------------------------------------------------------------------------
-- Section 12 : Row count validation
-- -----------------------------------------------------------------------------

SELECT
      'gold_vehicle_sales_summary'                               AS TARGET_TABLE_NAME
    , COUNT(1)                                                   AS TARGET_ROW_COUNT
    , COUNT(DISTINCT SUMMARY.DEALER_CODE)                        AS DISTINCT_DEALER_COUNT
    , COUNT(DISTINCT SUMMARY.SALE_MONTH)                         AS DISTINCT_MONTH_COUNT
    , MAX(SUMMARY.LOAD_TIMESTAMP)                                AS LATEST_LOAD_TIMESTAMP
FROM eagleeyelakebase_uc.automotive.gold_vehicle_sales_summary AS SUMMARY;

-- =============================================================================
-- End of gold_vehicle_sales_summary.sql
-- =============================================================================
