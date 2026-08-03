-- =====================================================================================
-- Airlines Domain Data Contract — Gold Layer Build
-- =====================================================================================
-- Purpose : Build 3 Gold (business-consumption) tables from the Silver tables
--           produced by build_silver_layer.py.
--
-- Gold tables created:
--   1. gold_flight_ops_daily_summary     <- silver_flight_operations
--                                            + silver_aircraft_maintenance
--                                            + silver_crew_duty
--   2. gold_passenger_revenue_360        <- silver_passenger_profile
--                                            + silver_revenue_ticketing
--                                            + silver_airport_baggage_events
--   3. gold_baggage_performance_by_airport <- silver_airport_baggage_events
--
-- Conventions used throughout this script:
--   - SQL keywords are UPPERCASE.
--   - No SELECT * — every column is explicitly listed.
--   - Explicit JOIN ... ON syntax only (no implicit comma joins).
--   - Descriptive table aliases (fo, am, cd, pp, rt, be) rather than single letters.
--   - COALESCE used for explicit NULL handling in all aggregates.
--   - CTEs (WITH clauses) used to break complex logic into readable steps.
--   - CREATE OR REPLACE TABLE used instead of DROP + CREATE, for idempotent reruns.
--   - Gold tables are partitioned on low-cardinality date columns.
-- =====================================================================================


-- =====================================================================================
-- 1. gold_flight_ops_daily_summary
--    Grain: one row per flight_date + origin_airport_code + destination_airport_code
--    Answers: on-time performance, delay severity mix, AOG exposure, and crew
--             rest-compliance risk per route per day.
-- =====================================================================================
CREATE OR REPLACE TABLE eagleeyelakebase_uc.airlines_gold.gold_flight_ops_daily_summary
COMMENT 'Daily flight operations summary by route: OTP, delay mix, AOG exposure, crew rest compliance.'
PARTITIONED BY (flight_date)
AS
WITH flight_metrics AS (
    SELECT
        fo.flight_date,
        fo.origin_airport_code,
        fo.destination_airport_code,
        fo.flight_id,
        fo.flight_number,
        fo.aircraft_registration,
        fo.flight_status,
        fo.is_delayed,
        fo.delay_bucket,
        COALESCE(fo.delay_minutes, 0) AS delay_minutes,
        CASE WHEN fo.flight_status = 'CANCELLED' THEN 1 ELSE 0 END AS is_cancelled_flag
    FROM eagleeyelakebase_uc.airlines_silver.silver_flight_operations AS fo
),

maintenance_by_aircraft_day AS (
    SELECT
        am.aircraft_registration,
        CAST(am.maintenance_start_ts AS DATE) AS maintenance_date,
        COUNT(CASE WHEN am.is_aog THEN 1 END) AS aog_event_count
    FROM eagleeyelakebase_uc.airlines_silver.silver_aircraft_maintenance AS am
    GROUP BY
        am.aircraft_registration,
        CAST(am.maintenance_start_ts AS DATE)
),

crew_compliance_by_flight_day AS (
    SELECT
        cd.flight_number,
        cd.flight_date,
        COUNT(CASE WHEN cd.is_rest_violation THEN 1 END) AS rest_violation_count,
        COUNT(DISTINCT cd.crew_id) AS crew_assigned_count
    FROM eagleeyelakebase_uc.airlines_silver.silver_crew_duty AS cd
    GROUP BY
        cd.flight_number,
        cd.flight_date
)

SELECT
    fm.flight_date,
    fm.origin_airport_code,
    fm.destination_airport_code,
    COUNT(DISTINCT fm.flight_id) AS total_flights,
    SUM(CASE WHEN fm.is_delayed THEN 1 ELSE 0 END) AS delayed_flight_count,
    SUM(fm.is_cancelled_flag) AS cancelled_flight_count,
    ROUND(AVG(fm.delay_minutes), 1) AS avg_delay_minutes,
    SUM(CASE WHEN fm.delay_bucket = 'MINOR' THEN 1 ELSE 0 END) AS minor_delay_count,
    SUM(CASE WHEN fm.delay_bucket = 'MODERATE' THEN 1 ELSE 0 END) AS moderate_delay_count,
    SUM(CASE WHEN fm.delay_bucket = 'SEVERE' THEN 1 ELSE 0 END) AS severe_delay_count,
    COALESCE(SUM(md.aog_event_count), 0) AS aog_event_count,
    COALESCE(SUM(cc.rest_violation_count), 0) AS crew_rest_violation_count,
    COALESCE(SUM(cc.crew_assigned_count), 0) AS crew_assigned_count,
    ROUND(
        100.0 * (COUNT(DISTINCT fm.flight_id) - SUM(fm.is_cancelled_flag)
            - SUM(CASE WHEN fm.is_delayed THEN 1 ELSE 0 END))
            / NULLIF(COUNT(DISTINCT fm.flight_id), 0),
        2
    ) AS on_time_performance_pct
FROM flight_metrics AS fm
LEFT JOIN maintenance_by_aircraft_day AS md
    ON fm.aircraft_registration = md.aircraft_registration
    AND fm.flight_date = md.maintenance_date
LEFT JOIN crew_compliance_by_flight_day AS cc
    ON fm.flight_number = cc.flight_number
    AND fm.flight_date = cc.flight_date
GROUP BY
    fm.flight_date,
    fm.origin_airport_code,
    fm.destination_airport_code;


-- =====================================================================================
-- 2. gold_passenger_revenue_360
--    Grain: one row per passenger_id
--    Answers: lifetime ticket revenue, refund exposure, and baggage mishandling
--             experience per passenger — a customer-value + service-quality view.
-- =====================================================================================
CREATE OR REPLACE TABLE eagleeyelakebase_uc.airlines_gold.gold_passenger_revenue_360
COMMENT 'Per-passenger 360 view: ticket revenue, refund exposure, baggage mishandling history.'
AS
WITH ticket_summary AS (
    SELECT
        rt.passenger_id,
        COUNT(DISTINCT rt.ticket_number) AS total_tickets,
        SUM(COALESCE(rt.net_revenue, 0)) AS total_net_revenue,
        SUM(COALESCE(rt.refund_amount, 0)) AS total_refund_amount,
        SUM(CASE WHEN rt.refund_status = 'COMPLETED' THEN 1 ELSE 0 END) AS completed_refund_count
    FROM eagleeyelakebase_uc.airlines_silver.silver_revenue_ticketing AS rt
    GROUP BY rt.passenger_id
),

baggage_summary AS (
    SELECT
        be.passenger_id,
        COUNT(DISTINCT be.event_id) AS total_baggage_events,
        SUM(CASE WHEN be.is_mishandled THEN 1 ELSE 0 END) AS mishandled_bag_count
    FROM eagleeyelakebase_uc.airlines_silver.silver_airport_baggage_events AS be
    WHERE be.passenger_id IS NOT NULL
    GROUP BY be.passenger_id
)

SELECT
    pp.passenger_id,
    pp.nationality,
    pp.booking_status,
    COALESCE(ts.total_tickets, 0) AS total_tickets,
    COALESCE(ts.total_net_revenue, 0.0) AS total_net_revenue,
    COALESCE(ts.total_refund_amount, 0.0) AS total_refund_amount,
    COALESCE(ts.completed_refund_count, 0) AS completed_refund_count,
    COALESCE(bs.total_baggage_events, 0) AS total_baggage_events,
    COALESCE(bs.mishandled_bag_count, 0) AS mishandled_bag_count,
    ROUND(
        100.0 * COALESCE(bs.mishandled_bag_count, 0)
            / NULLIF(COALESCE(bs.total_baggage_events, 0), 0),
        2
    ) AS mishandled_bag_rate_pct
FROM eagleeyelakebase_uc.airlines_silver.silver_passenger_profile AS pp
LEFT JOIN ticket_summary AS ts
    ON pp.passenger_id = ts.passenger_id
LEFT JOIN baggage_summary AS bs
    ON pp.passenger_id = bs.passenger_id;


-- =====================================================================================
-- 3. gold_baggage_performance_by_airport
--    Grain: one row per airport_code + event_date
--    Answers: daily baggage mishandling rate and average bag weight per airport,
--             for airport operations and IATA-style baggage KPI reporting.
-- =====================================================================================
CREATE OR REPLACE TABLE eagleeyelakebase_uc.airlines_gold.gold_baggage_performance_by_airport
COMMENT 'Daily baggage handling performance per airport: volume, mishandling rate, avg weight.'
PARTITIONED BY (event_date)
AS
SELECT
    be.airport_code,
    be.event_date,
    COUNT(DISTINCT be.bag_tag_number) AS total_bags_processed,
    SUM(CASE WHEN be.is_mishandled THEN 1 ELSE 0 END) AS mishandled_bag_count,
    ROUND(
        100.0 * SUM(CASE WHEN be.is_mishandled THEN 1 ELSE 0 END)
            / NULLIF(COUNT(DISTINCT be.bag_tag_number), 0),
        2
    ) AS mishandled_bag_rate_pct,
    ROUND(AVG(be.baggage_weight_kg), 2) AS avg_baggage_weight_kg,
    SUM(CASE WHEN be.baggage_status = 'LOST' THEN 1 ELSE 0 END) AS lost_bag_count,
    SUM(CASE WHEN be.baggage_status = 'DELAYED' THEN 1 ELSE 0 END) AS delayed_bag_count,
    SUM(CASE WHEN be.baggage_status = 'CLAIMED' THEN 1 ELSE 0 END) AS claimed_bag_count
FROM eagleeyelakebase_uc.airlines_silver.silver_airport_baggage_events AS be
WHERE be.bag_tag_number IS NOT NULL
GROUP BY
    be.airport_code,
    be.event_date;


-- =====================================================================================
-- Ad-hoc debug query (kept here for now, not part of the Gold build above)
-- =====================================================================================
select *
from eagleeyelakebase_uc.airlines_silver.silver_revenue_ticketing t, eagleeyelakebase_uc.airlines_silver.silver_passenger_profile p
where t.passenger_id = p.passenger_id
and t.fare_amount > 500
order by t.fare_amount;
