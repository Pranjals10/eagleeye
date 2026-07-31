"""Silver layer builder for the Airlines Domain Data Contract.

This module reads Bronze-layer Delta tables (raw, untransformed ingestion
from source systems) and produces cleansed, deduplicated, PII-masked
Silver-layer Delta tables:

    1. silver_passenger_profile     <- passenger_reservation_bronze
    2. silver_flight_operations     <- flight_operations_bronze
    3. silver_airport_baggage_events<- airport_baggage_events_bronze
    4. silver_revenue_ticketing     <- revenue_ticketing_bronze
    5. silver_aircraft_maintenance  <- aircraft_maintenance_bronze
    6. silver_crew_duty             <- crew_operations_bronze

Design notes (aligned to the team's engineering checklist):
    - No hardcoded catalog/schema/table names: all environment-specific
      values come from Databricks widgets, so the same notebook runs
      unchanged across dev/uat/prod.
    - Writes are idempotent: each Silver table is produced via a
      Delta MERGE keyed on its natural/primary key, so re-running the
      job for the same batch does not create duplicates.
    - PII columns (per the bronze schema contract's `is_pii` flag) are
      masked with SHA-256 before landing in Silver.
    - All external calls (table reads/writes) are wrapped in
      try/except with specific exception types and contextual logging.

Author: Data Engineering (Big Data & EDW)
"""

# --- standard library ---
import logging
import sys
from dataclasses import dataclass
from typing import List, Optional

# --- third-party ---
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

logger = logging.getLogger("airlines_silver_layer")
logger.setLevel(logging.INFO)
if not logger.handlers:
    _handler = logging.StreamHandler(sys.stdout)
    _handler.setFormatter(
        logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    )
    logger.addHandler(_handler)

REQUIRED_RECORD_STATUS = "ACTIVE"
REST_HOURS_COMPLIANCE_THRESHOLD = 10.0
CERTIFICATION_EXPIRY_WARNING_DAYS = 30
DELAY_THRESHOLD_MINUTES = 15


@dataclass(frozen=True)
class PipelineConfig:
    """Resolved runtime configuration for a single pipeline run.

    Args:
        catalog: Unity Catalog name (e.g. 'eagleeyelakebase_uc').
        bronze_schema: Schema holding the raw Bronze tables.
        silver_schema: Schema the Silver tables will be written to.
        ingestion_batch_id: Identifier for this run, used for lineage.
    """

    catalog: str
    bronze_schema: str
    silver_schema: str
    ingestion_batch_id: str

    def bronze_table(self, table_name: str) -> str:
        """Return the fully-qualified three-part name of a Bronze table.

        Args:
            table_name: Bare table name, e.g. 'flight_operations_bronze'.

        Returns:
            Fully-qualified name, e.g. 'catalog.airlines.flight_operations_bronze'.
        """
        return f"{self.catalog}.{self.bronze_schema}.{table_name}"

    def silver_table(self, table_name: str) -> str:
        """Return the fully-qualified three-part name of a Silver table.

        Args:
            table_name: Bare table name, e.g. 'silver_flight_operations'.

        Returns:
            Fully-qualified name, e.g. 'catalog.airlines_silver.silver_flight_operations'.
        """
        return f"{self.catalog}.{self.silver_schema}.{table_name}"


def get_pipeline_config(spark: SparkSession) -> PipelineConfig:
    """Resolve pipeline configuration from Databricks widgets.

    Values are never hardcoded in the pipeline body; they are supplied by
    the calling job/workflow so the same notebook is portable across
    dev/uat/prod without code changes.

    Args:
        spark: Active SparkSession (used to access dbutils via the
            notebook context in a Databricks job).

    Returns:
        A populated PipelineConfig instance.
    """
    dbutils = _get_dbutils(spark)
    dbutils.widgets.text("catalog", "eagleeyelakebase_uc", "Unity Catalog")
    dbutils.widgets.text("bronze_schema", "airlines", "Bronze Schema")
    dbutils.widgets.text("silver_schema", "airlines_silver", "Silver Schema")
    dbutils.widgets.text("ingestion_batch_id", "", "Ingestion Batch Id")

    return PipelineConfig(
        catalog=dbutils.widgets.get("catalog"),
        bronze_schema=dbutils.widgets.get("bronze_schema"),
        silver_schema=dbutils.widgets.get("silver_schema"),
        ingestion_batch_id=dbutils.widgets.get("ingestion_batch_id") or "manual_run",
    )


def _get_dbutils(spark: SparkSession):
    """Return the dbutils handle for the active Databricks context.

    Args:
        spark: Active SparkSession.

    Returns:
        The dbutils object.

    Raises:
        RuntimeError: If dbutils cannot be resolved (e.g. running outside
            a Databricks runtime).
    """
    try:
        from pyspark.dbutils import DBUtils  # noqa: WPS433 (runtime-only import)

        return DBUtils(spark)
    except ImportError as exc:
        raise RuntimeError(
            "dbutils is only available inside a Databricks runtime."
        ) from exc


def read_bronze_table(spark: SparkSession, fully_qualified_name: str) -> DataFrame:
    """Read a Bronze Delta table and validate it is not empty.

    Args:
        spark: Active SparkSession.
        fully_qualified_name: Three-part table name (catalog.schema.table).

    Returns:
        The Bronze table as a DataFrame.

    Raises:
        AnalysisException: If the table does not exist or cannot be resolved.
        ValueError: If the table exists but contains zero rows.
    """
    try:
        df = spark.table(fully_qualified_name)
    except AnalysisException:
        logger.error("Bronze table not found: %s", fully_qualified_name)
        raise

    if df.limit(1).count() == 0:
        raise ValueError(f"Bronze table '{fully_qualified_name}' returned no rows.")

    logger.info("Read Bronze table %s", fully_qualified_name)
    return df


def _mask_pii_columns(df: DataFrame, pii_columns: List[str]) -> DataFrame:
    """Mask PII columns with a one-way SHA-256 hash.

    NULL values are preserved as NULL (COALESCE-safe) rather than being
    hashed into a misleading non-null value.

    Args:
        df: Source DataFrame.
        pii_columns: Column names to mask, if present in the DataFrame.

    Returns:
        DataFrame with the listed columns replaced by their hashed value.
    """
    for column_name in pii_columns:
        if column_name in df.columns:
            df = df.withColumn(
                column_name,
                F.when(
                    F.col(column_name).isNotNull(),
                    F.sha2(F.col(column_name).cast("string"), 256),
                ).otherwise(F.lit(None)),
            )
    return df


def _dedupe_latest_by(df: DataFrame, key_columns: List[str], order_column: str) -> DataFrame:
    """Keep only the most recently loaded record per business key.

    Args:
        df: Source DataFrame containing possible duplicate keys.
        key_columns: Columns that together form the natural/business key.
        order_column: Timestamp column used to determine "latest".

    Returns:
        DataFrame with exactly one row per distinct key_columns value.
    """
    from pyspark.sql.window import Window

    window_spec = Window.partitionBy(*key_columns).orderBy(F.col(order_column).desc())
    return (
        df.withColumn("_row_rank", F.row_number().over(window_spec))
        .filter(F.col("_row_rank") == 1)
        .drop("_row_rank")
    )


def build_silver_passenger_profile(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    """Build the Silver passenger profile table.

    Deduplicates on passenger_id (latest booking wins), masks PII columns
    flagged in the Bronze schema contract, and applies explicit NULL
    handling on booking_status.

    Args:
        spark: Active SparkSession.
        config: Resolved pipeline configuration.

    Returns:
        Cleansed, PII-masked passenger profile DataFrame.
    """
    bronze_df = read_bronze_table(spark, config.bronze_table("passenger_reservation_bronze"))

    active_df = bronze_df.filter(F.col("record_status") == REQUIRED_RECORD_STATUS)
    deduped_df = _dedupe_latest_by(active_df, ["passenger_id"], "booking_timestamp")

    selected_df = deduped_df.select(
        "passenger_id",
        "pnr",
        "booking_id",
        "passenger_name",
        "middle_name",
        "date_of_birth",
        "nationality",
        "passport_number",
        "email",
        "phone_number",
        "frequent_flyer_number",
        F.coalesce(F.col("booking_status"), F.lit("UNKNOWN")).alias("booking_status"),
        "booking_timestamp",
        "flight_number",
        "source_system",
    ).withColumn("silver_load_ts", F.current_timestamp()).withColumn(
        "silver_ingestion_batch_id", F.lit(config.ingestion_batch_id)
    )

    pii_columns = [
        "passenger_name",
        "middle_name",
        "date_of_birth",
        "passport_number",
        "email",
        "phone_number",
        "frequent_flyer_number",
    ]
    return _mask_pii_columns(selected_df, pii_columns)


def build_silver_flight_operations(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    """Build the Silver flight operations table.

    Filters to active records, validates scheduled-time chronology, and
    derives operational KPIs (is_delayed, delay_bucket, block_time_minutes)
    used downstream by the Gold flight-ops summary.

    Args:
        spark: Active SparkSession.
        config: Resolved pipeline configuration.

    Returns:
        Cleansed flight operations DataFrame with derived KPI columns.
    """
    bronze_df = read_bronze_table(spark, config.bronze_table("flight_operations_bronze"))

    active_df = bronze_df.filter(F.col("record_status") == REQUIRED_RECORD_STATUS)

    chronology_valid_df = active_df.withColumn(
        "is_chronology_valid",
        F.col("scheduled_arrival_ts") > F.col("scheduled_departure_ts"),
    )

    enriched_df = (
        chronology_valid_df.withColumn(
            "delay_minutes_clean",
            F.when(F.col("delay_minutes") < 0, F.lit(0)).otherwise(
                F.coalesce(F.col("delay_minutes"), F.lit(0))
            ),
        )
        .withColumn(
            "is_delayed", F.col("delay_minutes_clean") >= F.lit(DELAY_THRESHOLD_MINUTES)
        )
        .withColumn(
            "delay_bucket",
            F.when(F.col("delay_minutes_clean") == 0, F.lit("ON_TIME"))
            .when(F.col("delay_minutes_clean") < 30, F.lit("MINOR"))
            .when(F.col("delay_minutes_clean") < 120, F.lit("MODERATE"))
            .otherwise(F.lit("SEVERE")),
        )
        .withColumn(
            "block_time_minutes",
            F.when(
                F.col("actual_departure_ts").isNotNull()
                & F.col("actual_arrival_ts").isNotNull(),
                (
                    F.unix_timestamp("actual_arrival_ts")
                    - F.unix_timestamp("actual_departure_ts")
                )
                / 60,
            ),
        )
    )

    selected_df = enriched_df.select(
        "flight_id",
        "flight_number",
        "flight_date",
        "origin_airport_code",
        "destination_airport_code",
        "scheduled_departure_ts",
        "scheduled_arrival_ts",
        "actual_departure_ts",
        "actual_arrival_ts",
        "aircraft_registration",
        "flight_status",
        "delay_code",
        "delay_minutes_clean",
        "is_delayed",
        "delay_bucket",
        "block_time_minutes",
        "cancellation_reason",
        "is_chronology_valid",
        "source_system",
    ).withColumnRenamed("delay_minutes_clean", "delay_minutes").withColumn(
        "silver_load_ts", F.current_timestamp()
    ).withColumn("silver_ingestion_batch_id", F.lit(config.ingestion_batch_id))

    return selected_df


def build_silver_airport_baggage_events(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    """Build the Silver airport & baggage events table.

    Normalizes free-text status/type casing and derives an
    `is_mishandled` flag used by the Gold baggage performance table.

    Args:
        spark: Active SparkSession.
        config: Resolved pipeline configuration.

    Returns:
        Cleansed airport/baggage event DataFrame.
    """
    bronze_df = read_bronze_table(spark, config.bronze_table("airport_baggage_events_bronze"))

    active_df = bronze_df.filter(F.col("record_status") == REQUIRED_RECORD_STATUS)

    normalized_df = active_df.withColumn(
        "event_type", F.upper(F.trim(F.col("event_type")))
    ).withColumn(
        "baggage_status", F.upper(F.trim(F.col("baggage_status")))
    )

    mishandled_statuses = ["LOST", "DELAYED", "MISHANDLED"]
    enriched_df = normalized_df.withColumn(
        "is_mishandled", F.col("baggage_status").isin(mishandled_statuses)
    ).withColumn("event_date", F.to_date(F.col("event_timestamp")))

    selected_df = enriched_df.select(
        "event_id",
        "pnr",
        "passenger_id",
        "flight_number",
        "airport_code",
        "event_type",
        "event_timestamp",
        "event_date",
        "bag_tag_number",
        "baggage_weight_kg",
        "baggage_status",
        "is_mishandled",
        "gate_number",
        "boarding_pass_number",
        "seat_number",
        "special_service_request",
        "source_system",
    ).withColumn("silver_load_ts", F.current_timestamp()).withColumn(
        "silver_ingestion_batch_id", F.lit(config.ingestion_batch_id)
    )

    return _mask_pii_columns(selected_df, ["passenger_id"])


def build_silver_revenue_ticketing(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    """Build the Silver revenue & ticketing table.

    Derives net_revenue (fare + tax - refund) with explicit NULL-safe
    arithmetic, and flags negative-fare anomalies for downstream review.

    Args:
        spark: Active SparkSession.
        config: Resolved pipeline configuration.

    Returns:
        Cleansed revenue/ticketing DataFrame with a net_revenue column.
    """
    bronze_df = read_bronze_table(spark, config.bronze_table("revenue_ticketing_bronze"))

    active_df = bronze_df.filter(F.col("record_status") == REQUIRED_RECORD_STATUS)

    enriched_df = active_df.withColumn(
        "net_revenue",
        F.coalesce(F.col("fare_amount"), F.lit(0.0))
        + F.coalesce(F.col("tax_amount"), F.lit(0.0))
        - F.coalesce(F.col("refund_amount"), F.lit(0.0)),
    ).withColumn("is_negative_fare", F.col("fare_amount") < 0)

    selected_df = enriched_df.select(
        "ticket_number",
        "pnr",
        "passenger_id",
        "fare_amount",
        "currency_code",
        "tax_amount",
        "refund_amount",
        "net_revenue",
        "is_negative_fare",
        "payment_method",
        "payment_status",
        "refund_status",
        "ancillary_service_code",
        "ticket_issue_timestamp",
        "source_system",
    ).withColumn("silver_load_ts", F.current_timestamp()).withColumn(
        "silver_ingestion_batch_id", F.lit(config.ingestion_batch_id)
    )

    return _mask_pii_columns(selected_df, ["passenger_id"])


def build_silver_aircraft_maintenance(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    """Build the Silver aircraft maintenance table.

    Derives maintenance_duration_hours, an AOG flag, and a
    certification-expiring-soon flag (within the configured warning
    window) to support fleet airworthiness monitoring downstream.

    Args:
        spark: Active SparkSession.
        config: Resolved pipeline configuration.

    Returns:
        Cleansed aircraft maintenance DataFrame with derived flags.
    """
    bronze_df = read_bronze_table(spark, config.bronze_table("aircraft_maintenance_bronze"))

    active_df = bronze_df.filter(F.col("record_status") == REQUIRED_RECORD_STATUS)

    enriched_df = (
        active_df.withColumn(
            "maintenance_duration_hours",
            F.when(
                F.col("maintenance_start_ts").isNotNull()
                & F.col("maintenance_end_ts").isNotNull(),
                (
                    F.unix_timestamp("maintenance_end_ts")
                    - F.unix_timestamp("maintenance_start_ts")
                )
                / 3600,
            ),
        )
        .withColumn("is_aog", F.col("maintenance_status") == F.lit("AOG"))
        .withColumn(
            "is_certification_expiring_soon",
            F.col("certification_expiry_date")
            <= F.date_add(F.current_date(), CERTIFICATION_EXPIRY_WARNING_DAYS),
        )
    )

    selected_df = enriched_df.select(
        "aircraft_registration",
        "aircraft_type",
        "fleet_id",
        "maintenance_event_id",
        "maintenance_type",
        "maintenance_status",
        "is_aog",
        "maintenance_start_ts",
        "maintenance_end_ts",
        "maintenance_duration_hours",
        "fuel_quantity_kg",
        "defect_code",
        "seat_capacity",
        "aircraft_age_years",
        "certification_expiry_date",
        "is_certification_expiring_soon",
        "source_system",
    ).withColumn("silver_load_ts", F.current_timestamp()).withColumn(
        "silver_ingestion_batch_id", F.lit(config.ingestion_batch_id)
    )

    return selected_df


def build_silver_crew_duty(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    """Build the Silver crew duty table.

    Derives duty_duration_hours and a regulatory rest-hours compliance
    flag, and masks crew PII columns (crew_name, license_number).

    Args:
        spark: Active SparkSession.
        config: Resolved pipeline configuration.

    Returns:
        Cleansed crew duty DataFrame with compliance flags.
    """
    bronze_df = read_bronze_table(spark, config.bronze_table("crew_operations_bronze"))

    active_df = bronze_df.filter(F.col("record_status") == REQUIRED_RECORD_STATUS)

    enriched_df = active_df.withColumn(
        "duty_duration_hours",
        F.when(
            F.col("duty_start_ts").isNotNull() & F.col("duty_end_ts").isNotNull(),
            (F.unix_timestamp("duty_end_ts") - F.unix_timestamp("duty_start_ts")) / 3600,
        ),
    ).withColumn(
        "is_rest_violation",
        F.coalesce(F.col("rest_hours"), F.lit(0.0)) < REST_HOURS_COMPLIANCE_THRESHOLD,
    )

    selected_df = enriched_df.select(
        "crew_id",
        "crew_name",
        "crew_role",
        "flight_number",
        "flight_date",
        "duty_start_ts",
        "duty_end_ts",
        "duty_duration_hours",
        "duty_status",
        "license_number",
        "certification_type",
        "certification_expiry_date",
        "crew_base_airport",
        "rest_hours",
        "is_rest_violation",
        "flight_assignment_id",
        "source_system",
    ).withColumn("silver_load_ts", F.current_timestamp()).withColumn(
        "silver_ingestion_batch_id", F.lit(config.ingestion_batch_id)
    )

    return _mask_pii_columns(selected_df, ["crew_name", "license_number"])


def write_silver_table(
    spark: SparkSession,
    df: DataFrame,
    target_table: str,
    merge_key_columns: List[str],
    partition_columns: Optional[List[str]] = None,
) -> None:
    """Idempotently write a Silver DataFrame via Delta MERGE (upsert).

    Creates the table on first run (CREATE OR REPLACE semantics via
    `saveAsTable`) and upserts on subsequent runs, so re-running the same
    batch never produces duplicate rows.

    Args:
        spark: Active SparkSession.
        df: The Silver DataFrame to persist.
        target_table: Fully-qualified target table name.
        merge_key_columns: Columns forming the natural key to merge on.
        partition_columns: Optional partition columns for the target table.

    Raises:
        AnalysisException: If the write or merge operation fails.
    """
    try:
        if not spark.catalog.tableExists(target_table):
            writer = df.write.format("delta").mode("overwrite").option(
                "overwriteSchema", "true"
            )
            if partition_columns:
                writer = writer.partitionBy(*partition_columns)
            writer.saveAsTable(target_table)
            logger.info("Created Silver table %s (%d rows)", target_table, df.count())
            return

        delta_target = DeltaTable.forName(spark, target_table)
        merge_condition = " AND ".join(
            f"target.{col} = source.{col}" for col in merge_key_columns
        )
        (
            delta_target.alias("target")
            .merge(df.alias("source"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        logger.info("Merged into Silver table %s", target_table)
    except AnalysisException:
        logger.error("Failed to write Silver table %s", target_table, exc_info=True)
        raise


def run_silver_pipeline(spark: SparkSession) -> None:
    """Entry point: build and persist all six Silver tables.

    Args:
        spark: Active SparkSession.
    """
    config = get_pipeline_config(spark)
    logger.info("Starting Silver layer build | batch_id=%s", config.ingestion_batch_id)

    builders = [
        (build_silver_passenger_profile, "silver_passenger_profile", ["passenger_id"], None),
        (
            build_silver_flight_operations,
            "silver_flight_operations",
            ["flight_id", "flight_date"],
            ["flight_date"],
        ),
        (
            build_silver_airport_baggage_events,
            "silver_airport_baggage_events",
            ["event_id"],
            ["event_date"],
        ),
        (build_silver_revenue_ticketing, "silver_revenue_ticketing", ["ticket_number"], None),
        (
            build_silver_aircraft_maintenance,
            "silver_aircraft_maintenance",
            ["aircraft_registration", "maintenance_event_id"],
            None,
        ),
        (
            build_silver_crew_duty,
            "silver_crew_duty",
            ["crew_id", "flight_assignment_id"],
            ["flight_date"],
        ),
    ]

    for build_fn, table_name, merge_keys, partitions in builders:
        try:
            silver_df = build_fn(spark, config)
            write_silver_table(
                spark,
                silver_df,
                config.silver_table(table_name),
                merge_key_columns=merge_keys,
                partition_columns=partitions,
            )
        except (AnalysisException, ValueError):
            logger.error("Skipping %s due to a build/write failure.", table_name, exc_info=True)
            continue

    logger.info("Silver layer build complete.")


if __name__ == "__main__":
    active_spark = SparkSession.builder.appName("airlines_silver_layer_build").getOrCreate()
    run_silver_pipeline(active_spark)
