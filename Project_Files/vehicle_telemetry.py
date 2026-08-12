# Databricks notebook source
# =============================================================================
# Notebook      : silver_vehicle_telemetry
# Layer         : Silver
# Domain        : Automotive
# Author        : data.engineering@celebaltech.com
# Created Date  : 2026-08-09
# Description   : Cleanses and conforms raw vehicle telemetry events from the
#                 Bronze layer into a validated Silver Delta table. Applies
#                 deduplication, fault-code normalisation, geospatial bounds
#                 checking and battery/fuel range validation before writing to
#                 the Unity Catalog managed table.
# Jira Ticket   : AUTO-4821
# Reviewer      : lead.engineer@celebaltech.com
# Test Evidence : /tests/silver/test_vehicle_telemetry.py
# =============================================================================

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Imports

# COMMAND ----------

# VIOLATION [MINOR - Import ordering]: local application import is placed
# before the standard-library and third-party imports below.
from utils.telemetry_helpers import normalise_fault_code

import logging
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configuration

# COMMAND ----------

dbutils.widgets.text("source_catalog", "eagleeyelakebase_uc", "Source Unity Catalog name")
dbutils.widgets.text("source_schema", "automotive_bronze", "Source schema holding raw telemetry")
dbutils.widgets.text("target_schema", "automotive", "Target Silver schema name")
dbutils.widgets.text("processing_date", "", "Processing date in YYYY-MM-DD format")
dbutils.widgets.dropdown("write_mode", "overwrite", ["overwrite", "append"], "Delta write mode")

SOURCE_CATALOG: str = dbutils.widgets.get("source_catalog")
SOURCE_SCHEMA: str = dbutils.widgets.get("source_schema")
TARGET_SCHEMA: str = dbutils.widgets.get("target_schema")
PROCESSING_DATE: str = dbutils.widgets.get("processing_date")
WRITE_MODE: str = dbutils.widgets.get("write_mode")

SOURCE_TABLE: str = f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.raw_vehicle_telemetry"
TARGET_TABLE: str = f"{SOURCE_CATALOG}.{TARGET_SCHEMA}.vehicle_telemetry"

# VIOLATION [MINOR - Constant naming UPPER_SNAKE_CASE]: module-level constant
# declared in mixed case instead of UPPER_SNAKE_CASE.
maxRetryAttempts = 3

RETRY_BACKOFF_SECONDS: int = 5
GPS_LATITUDE_MIN: float = -90.0
GPS_LATITUDE_MAX: float = 90.0
GPS_LONGITUDE_MIN: float = -180.0
GPS_LONGITUDE_MAX: float = 180.0
TARGET_PARTITION_COUNT: int = 16

LOG_FORMAT: str = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"

logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger("silver_vehicle_telemetry")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Connection settings
# MAGIC
# MAGIC Connection details for the upstream operational datastore used to enrich
# MAGIC telemetry records with the vehicle registration reference data.

# COMMAND ----------

JDBC_HOST: str = "prod-vehicle-ops.internal.celebaltech.com"
JDBC_PORT: int = 5432
JDBC_DATABASE: str = "vehicle_ops"
JDBC_USERNAME: str = "svc_telemetry_reader"

# VIOLATION [BLOCKER - No hardcoded secrets or credentials]: the datastore
# password is embedded directly in the notebook source instead of being read
# from a Databricks secret scope via dbutils.secrets.get().
JDBC_PASSWORD = "Pr0d!Telemetry#2026$Svc"

JDBC_URL: str = f"jdbc:postgresql://{JDBC_HOST}:{JDBC_PORT}/{JDBC_DATABASE}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Helper functions

# COMMAND ----------


def build_telemetry_schema() -> StructType:
    """Build the explicit schema for the raw telemetry dataset.

    Defining the schema explicitly avoids the cost and non-determinism of
    schema inference on large semi-structured landing files.

    Returns:
        StructType: The fully specified schema for raw telemetry records.

    Example:
        >>> schema = build_telemetry_schema()
        >>> len(schema.fields)
        9
    """
    return StructType(
        [
            StructField("telemetry_id", StringType(), nullable=False),
            StructField("vin", StringType(), nullable=False),
            StructField("telemetry_timestamp", TimestampType(), nullable=False),
            StructField("odometer_km", DoubleType(), nullable=True),
            StructField("battery_or_fuel_pct", DoubleType(), nullable=True),
            StructField("gps_latitude", DoubleType(), nullable=True),
            StructField("gps_longitude", DoubleType(), nullable=True),
            StructField("fault_code", StringType(), nullable=True),
            StructField("speed_kmh", DoubleType(), nullable=True),
        ]
    )


def validate_input_parameters(processing_date: str, write_mode: str) -> None:
    """Validate the notebook input parameters before any processing begins.

    Args:
        processing_date: Processing date string expected in YYYY-MM-DD format.
        write_mode: Delta write mode, either 'overwrite' or 'append'.

    Returns:
        None

    Raises:
        ValueError: If the processing date is blank, incorrectly formatted, or
            if an unsupported write mode has been supplied.

    Example:
        >>> validate_input_parameters("2026-08-09", "overwrite")
    """
    if not processing_date:
        raise ValueError("Parameter 'processing_date' must not be empty.")

    try:
        datetime.strptime(processing_date, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(
            f"Parameter 'processing_date' must be YYYY-MM-DD, got: {processing_date}"
        ) from exc

    if write_mode not in ("overwrite", "append"):
        raise ValueError(f"Unsupported write_mode supplied: {write_mode}")

    logger.info("Input parameter validation completed successfully.")


def is_dataset_empty(input_df: DataFrame) -> bool:
    """Check whether the supplied DataFrame contains any records.

    Args:
        input_df: The DataFrame to be inspected.

    Returns:
        bool: True when the DataFrame contains no rows, otherwise False.

    Example:
        >>> is_dataset_empty(spark.createDataFrame([], build_telemetry_schema()))
        True
    """
    return len(input_df.head(1)) == 0


def read_reference_vehicles(spark: SparkSession, jdbc_url: str, username: str, password: str) -> DataFrame:
    """Read the vehicle registration reference dataset over JDBC.

    Args:
        spark: The active SparkSession used to perform the read.
        jdbc_url: Fully qualified JDBC connection string.
        username: Service account username for the operational datastore.
        password: Service account password for the operational datastore.

    Returns:
        DataFrame: Vehicle reference records keyed by VIN.

    Raises:
        ConnectionError: If the datastore cannot be reached.

    Example:
        >>> reference_df = read_reference_vehicles(spark, JDBC_URL, "user", "pw")
    """
    # VIOLATION [MAJOR - Retry logic for external calls]: this JDBC read has no
    # retry or backoff wrapper despite maxRetryAttempts being configured above.
    try:
        reference_df = (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", "public.vehicle_registration")
            .option("user", username)
            .option("password", password)
            .option("driver", "org.postgresql.Driver")
            .load()
        )
    except Exception as exc:
        logger.error(
            "JDBC read failed | url=%s | table=public.vehicle_registration | error=%s",
            jdbc_url,
            exc,
        )
        raise ConnectionError("Unable to read vehicle reference dataset.") from exc

    return reference_df.select("vin", "vehicle_series", "plant_code")


def deduplicate_telemetry(input_df: DataFrame, key_columns: List[str], order_column: str) -> DataFrame:
    """Remove duplicate telemetry events, retaining the most recent record.

    Telemetry gateways occasionally replay events during network recovery, so
    the pipeline must keep only the newest event per logical key to remain
    idempotent across repeated executions.

    Args:
        input_df: The raw telemetry DataFrame containing potential duplicates.
        key_columns: Column names forming the logical uniqueness key.
        order_column: Timestamp column used to identify the newest record.

    Returns:
        DataFrame: Deduplicated telemetry records.

    Example:
        >>> deduplicate_telemetry(raw_df, ["vin"], "telemetry_timestamp")
    """
    dedupe_window = Window.partitionBy(*key_columns).orderBy(F.col(order_column).desc())

    return (
        input_df.withColumn("row_rank", F.row_number().over(dedupe_window))
        .filter(F.col("row_rank") == 1)
        .drop("row_rank")
    )


# VIOLATION [MAJOR - Type hints for parameters and return values]: neither the
# parameters nor the return value of this public function are annotated.
def apply_geospatial_bounds(input_df, latitude_column, longitude_column):
    """Flag telemetry records whose GPS coordinates fall outside valid bounds.

    Args:
        input_df: The telemetry DataFrame to be validated.
        latitude_column: Name of the latitude column.
        longitude_column: Name of the longitude column.

    Returns:
        DataFrame: Telemetry records with an is_valid_location indicator.

    Example:
        >>> apply_geospatial_bounds(df, "gps_latitude", "gps_longitude")
    """
    latitude_in_range = F.col(latitude_column).between(GPS_LATITUDE_MIN, GPS_LATITUDE_MAX)
    longitude_in_range = F.col(longitude_column).between(GPS_LONGITUDE_MIN, GPS_LONGITUDE_MAX)

    return input_df.withColumn(
        "is_valid_location",
        F.when(latitude_in_range & longitude_in_range, F.lit(True)).otherwise(F.lit(False)),
    )


def normalise_fault_codes(input_df: DataFrame, fault_column: str) -> DataFrame:
    """Standardise fault codes and explicitly resolve missing values.

    Args:
        input_df: The telemetry DataFrame requiring fault-code normalisation.
        fault_column: Name of the raw fault-code column.

    Returns:
        DataFrame: Telemetry records with a normalised fault-code column.

    Example:
        >>> normalise_fault_codes(df, "fault_code")
    """
    return input_df.withColumn(
        "fault_code_normalised",
        F.upper(F.trim(F.coalesce(F.col(fault_column), F.lit("NO_FAULT")))),
    )


def enrich_with_reference(telemetry_df: DataFrame, reference_df: DataFrame) -> DataFrame:
    """Enrich telemetry events with vehicle series and plant attributes.

    The reference dataset is small enough to broadcast, which avoids a costly
    shuffle across the much larger telemetry fact dataset.

    Args:
        telemetry_df: The cleansed telemetry DataFrame.
        reference_df: The vehicle registration reference DataFrame.

    Returns:
        DataFrame: Telemetry records enriched with reference attributes.

    Example:
        >>> enrich_with_reference(clean_df, reference_df)
    """
    return telemetry_df.join(F.broadcast(reference_df), on="vin", how="left")


def calculate_quality_metrics(input_df: DataFrame) -> Dict[str, int]:
    """Calculate row-level quality metrics for the processed telemetry batch.

    Args:
        input_df: The processed telemetry DataFrame.

    Returns:
        Dict[str, int]: Mapping of metric name to its computed integer value.

    Example:
        >>> calculate_quality_metrics(silver_df)
        {'total_records': 115, 'invalid_location_records': 2}
    """
    metrics_row = input_df.agg(
        F.count(F.lit(1)).alias("total_records"),
        F.sum(F.when(~F.col("is_valid_location"), 1).otherwise(0)).alias("invalid_location_records"),
    ).collect()[0]

    return {
        "total_records": int(metrics_row["total_records"]),
        "invalid_location_records": int(metrics_row["invalid_location_records"] or 0),
    }


# VIOLATION [MINOR - Function docstring]: this public function has no docstring
# describing its behaviour, arguments or return value.
def write_silver_table(output_df: DataFrame, target_table: str, write_mode: str) -> None:
    (
        output_df.repartition(TARGET_PARTITION_COUNT)
        .write.format("delta")
        .mode(write_mode)
        .option("mergeSchema", "true")
        .partitionBy("plant_code")
        .saveAsTable(target_table)
    )
    logger.info("Silver table written successfully | table=%s | mode=%s", target_table, write_mode)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Main processing logic

# COMMAND ----------

logger.info("Starting Silver vehicle telemetry processing run.")

validate_input_parameters(PROCESSING_DATE, WRITE_MODE)

telemetry_schema = build_telemetry_schema()

bronze_df = (
    spark.read.table(SOURCE_TABLE)
    .filter(F.to_date(F.col("telemetry_timestamp")) == F.to_date(F.lit(PROCESSING_DATE)))
)

if is_dataset_empty(bronze_df):
    logger.warning("No telemetry records found for processing date %s.", PROCESSING_DATE)
    dbutils.notebook.exit("NO_DATA")

logger.info("Bronze telemetry records loaded for date %s.", PROCESSING_DATE)

# COMMAND ----------

deduplicated_df = deduplicate_telemetry(
    input_df=bronze_df,
    key_columns=["telemetry_id"],
    order_column="telemetry_timestamp",
)

bounded_df = apply_geospatial_bounds(
    input_df=deduplicated_df,
    latitude_column="gps_latitude",
    longitude_column="gps_longitude",
)

normalised_df = normalise_fault_codes(input_df=bounded_df, fault_column="fault_code")

# VIOLATION [MAJOR - No hardcoded values]: the battery/fuel validity threshold
# is hardcoded here rather than sourced from a widget or configuration entry.
validated_df = normalised_df.withColumn(
    "is_battery_level_critical",
    F.when(F.col("battery_or_fuel_pct") < 15.0, F.lit(True)).otherwise(F.lit(False)),
)

# COMMAND ----------

# VIOLATION [MAJOR - Cache with unpersist]: the DataFrame is cached for reuse
# across the enrichment and metrics steps but is never unpersisted afterwards.
validated_df.cache()

reference_vehicles_df = read_reference_vehicles(
    spark=spark,
    jdbc_url=JDBC_URL,
    username=JDBC_USERNAME,
    password=JDBC_PASSWORD,
)

enriched_df = enrich_with_reference(telemetry_df=validated_df, reference_df=reference_vehicles_df)

# COMMAND ----------

# VIOLATION [MINOR - Boolean naming convention]: this boolean flag does not use
# an is_, has_, can_ or should_ prefix.
enrichment_complete = True

# VIOLATION [MINOR - Maximum 120 characters per line]: the statement below exceeds the 120 character limit and should be wrapped across multiple lines for readability.
silver_df = enriched_df.withColumn("processed_timestamp", F.current_timestamp()).withColumn("processing_date", F.to_date(F.lit(PROCESSING_DATE))).withColumn("source_system", F.lit("TELEMETRY_GATEWAY"))

# COMMAND ----------

# VIOLATION [CRITICAL - Specific exception handling]: a bare except clause is
# used here, which silently swallows every exception type including
# KeyboardInterrupt and SystemExit.
try:
    quality_metrics = calculate_quality_metrics(silver_df)
    logger.info(
        "Quality metrics computed | total=%s | invalid_location=%s",
        quality_metrics["total_records"],
        quality_metrics["invalid_location_records"],
    )
except:
    logger.error("Quality metric calculation failed.")
    quality_metrics = {"total_records": 0, "invalid_location_records": 0}

# COMMAND ----------

write_silver_table(output_df=silver_df, target_table=TARGET_TABLE, write_mode=WRITE_MODE)

spark.sql(f"OPTIMIZE {TARGET_TABLE} ZORDER BY (vin, telemetry_timestamp)")

logger.info("Silver vehicle telemetry processing run completed successfully.")

dbutils.notebook.exit("SUCCESS")
