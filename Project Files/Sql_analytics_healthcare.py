"""
Silver Layer Transformation Pipeline - Healthcare Domain

Builds silver tables from bronze clinical/patient tables:
patients, encounter, encounter_admissions, clinical_event,
diagnosis, clinical_orders, patient_visits.

NOTE: This file is a seeded test fixture for a code-quality inspector.
It intentionally contains BOTH compliant code and rule violations
across naming, documentation, hardcoding, performance, reliability,
and security categories so scan coverage can be validated.
"""

from pyspark.sql import SparkSession
import os
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import logging

logger = logging.getLogger(__name__)

# --- Constants ---------------------------------------------------------
MAX_RETRY_COUNT = 3  # correct: UPPER_SNAKE_CASE
db_password = "P@ssw0rd123!"  # VIOLATION: hardcoded secret, wrong constant case


# --- Classes -------------------------------------------------------------
class SilverTableBuilder:  # correct: PascalCase
    """Utility wrapper for building silver tables.

    Args:
        spark (SparkSession): active Spark session.

    Returns:
        None

    Example:
        builder = SilverTableBuilder(spark)
    """

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark


class jdbc_helper:  # VIOLATION: class name should be PascalCase
    def __init__(self, url):
        self.url = url


# --- Helper functions used before definition (VIOLATION) -----------------
def build_all_silver_tables(spark: SparkSession) -> None:
    """Run the full silver build pipeline.

    Args:
        spark (SparkSession): active spark session.

    Returns:
        None
    """
    # calls a helper defined further below in the file - VIOLATION
    df = load_bronze_table(spark, "patients")
    df.show()  # VIOLATION: unnecessary Spark action left in pipeline code


def load_bronze_table(spark: SparkSession, table_name: str):
    """Load a bronze table by name.

    Args:
        spark (SparkSession): active spark session.
        table_name (str): name of bronze table to load.

    Returns:
        DataFrame: loaded bronze table.

    Raises:
        Exception: if the table cannot be read.
    """
    try:
        return spark.table(f"bronze.{table_name}")
    except Exception as e:
        logger.error(f"Failed to load bronze table {table_name}: {e}")
        raise


# --- Silver table 1: patients (PII masking demo) --------------------------
def build_silver_patients(spark: SparkSession):
    """Build silver_patients with PII masking applied.

    Args:
        spark (SparkSession): active spark session.

    Returns:
        DataFrame: cleaned, masked patient dimension.
    """
    bronze_df = spark.table("hive_metastore.bronze.patients")  # VIOLATION: hive_metastore usage instead of Unity Catalog

    is_active = F.col("active_ind") == 1  # correct: boolean uses is_ prefix

    silver_df = (
        bronze_df.filter(is_active)
        .withColumn("name_last_masked", F.sha2(F.col("name_last"), 256))  # correct: PII masked
        .withColumn("name_first", F.col("name_first"))  # VIOLATION: PII left unmasked
        .withColumn("birth_dt_tm", F.to_timestamp("birth_dt_tm"))
        .select(
            "person_id", "name_last_masked", "name_first", "birth_dt_tm",
            "sex_cd", "nationality_cd", "marital_type_cd", "deceased_cd",
            "mrn", "active_ind"
        )
    )
    return silver_df


# --- Silver table 2: encounters --------------------------------------------
def build_silver_encounters(spark: SparkSession):
    """Build silver_encounters from bronze encounter table.

    Args:
        spark (SparkSession): active spark session.

    Returns:
        DataFrame: cleaned encounter table partitioned for downstream use.
    """
    bronze_df = spark.table("main.bronze.encounter")  # correct: Unity Catalog 3-level namespace

    silver_df = (
        bronze_df.filter(F.col("active_ind") == 1)
        .withColumn("admit_dt_tm", F.to_timestamp("admit_dt_tm"))
        .withColumn("disch_dt_tm", F.to_timestamp("disch_dt_tm"))
        .dropDuplicates(["encntr_id"])
    )

    # correct: partition on low-cardinality column
    (
        silver_df.write.format("delta")
        .mode("overwrite")
        .partitionBy("loc_facility_cd")
        .option("mergeSchema", "true")
        .saveAsTable("main.silver.silver_encounters")
    )
    return silver_df


# --- Silver table 3: clinical events (VIOLATION-heavy function) -----------
def BuildClinicalEventsAndOrdersAndDiagnosis(spark):  # VIOLATION: camelCase name, no type hints, does 3 things (SRP)
    # this function intentionally violates single-responsibility by building
    # three separate silver tables at once
    events_df = spark.table("main.bronze.clinical_event").filter(F.col("result_status_cd") == 1)
    orders_df = spark.table("main.bronze.clinical_orders").filter(F.col("active_ind") == 1)
    diagnosis_df = spark.table("main.bronze.diagnosis").filter(F.col("active_ind") == 1)

    events_df.write.format("delta").mode("overwrite").saveAsTable("main.silver.silver_clinical_events")
    orders_df.write.format("delta").mode("overwrite").saveAsTable("main.silver.silver_clinical_orders")
    diagnosis_df.write.format("delta").mode("overwrite").saveAsTable("main.silver.silver_diagnosis")

    return events_df, orders_df, diagnosis_df


# --- Silver table 4: clinical events (clean, correct version) -------------
def build_silver_clinical_events(spark: SparkSession):
    """Build silver_clinical_events with partition pruning applied early.

    Args:
        spark (SparkSession): active spark session.

    Returns:
        DataFrame: filtered, deduplicated clinical events.
    """
    bronze_df = spark.table("main.bronze.clinical_event")

    # correct: early filter for partition pruning before further transforms
    filtered_df = bronze_df.filter(F.col("event_start_dt_tm") >= "2022-01-01")

    silver_df = (
        filtered_df.filter(F.col("result_status_cd") == 1)
        .dropDuplicates(["event_id"])
        .withColumn("event_start_dt_tm", F.to_timestamp("event_start_dt_tm"))
        .withColumn("event_end_dt_tm", F.to_timestamp("event_end_dt_tm"))
    )

    silver_df.cache()  # VIOLATION: cached without unpersist() later

    (
        silver_df.write.format("delta")
        .mode("overwrite")
        .partitionBy("event_cd")  # VIOLATION: high-cardinality partition column
        .saveAsTable("main.silver.silver_clinical_events_v2")
    )
    return silver_df


# --- Silver table 5: diagnosis (correct broadcast join example) -----------
def build_silver_diagnosis(spark: SparkSession) -> "DataFrame":
    """Build silver_diagnosis joined with a small reference table.

    Args:
        spark (SparkSession): active spark session.

    Returns:
        DataFrame: diagnosis records enriched with reference data.
    """
    diagnosis_df = spark.table("main.bronze.diagnosis").filter(F.col("active_ind") == 1)
    ref_df = spark.table("main.reference.diag_type_lookup")  # small dimension table

    # correct: broadcast join used for small lookup table
    enriched_df = diagnosis_df.join(F.broadcast(ref_df), "diag_type_cd", "left")

    (
        enriched_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")  # correct: schema evolution handled explicitly
        .saveAsTable("main.silver.silver_diagnosis_enriched")
    )
    return enriched_df


# --- Silver table 6: patient visits (loop with Spark action - VIOLATION) --
def build_silver_patient_visits(spark: SparkSession):
    """Build silver_patient_visits and validate row counts per department.

    Args:
        spark (SparkSession): active spark session.

    Returns:
        DataFrame: cleaned patient visits table.
    """
    bronze_df = spark.table("main.bronze.patient_visits")

    if bronze_df is None:  # correct: input validation before processing
        raise ValueError("patient_visits bronze table returned no data")

    silver_df = bronze_df.withColumnRenamed("pat_enc_csn_id", "encounter_csn_id")

    department_ids = [1, 2, 3, 4, 5]
    for dept_id in department_ids:
        row_count = silver_df.filter(F.col("department_id") == dept_id).count()  # VIOLATION: Spark action inside loop
        logger.info(f"department {dept_id} row count: {row_count}")

    silver_df.repartition(8).write.format("delta").mode("overwrite").saveAsTable(
        "main.silver.silver_patient_visits"
    )
    return silver_df


# --- JDBC connection example (secrets + exception handling) ---------------
def read_from_external_source(spark: SparkSession, table_name: str):
    """Read a table from an external JDBC source with retry-safe handling.

    Args:
        spark (SparkSession): active spark session.
        table_name (str): source table name to read.

    Returns:
        DataFrame: data read from the external source.
    """
    jdbc_url = f"jdbc:sqlserver://legacy-host:1433;database=clinical;password={db_password}"  # VIOLATION: hardcoded secret in config
    try:
        return (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", table_name)
            .load()
        )
    except Exception as read_error:  # correct: specific-ish handling with context logging
        logger.error(f"JDBC read failed for {table_name}: {read_error}")
        raise


def risky_lookup(spark, table_name):
    try:
        return spark.table(table_name)
    except:  # VIOLATION: bare except, no context logging
        return None


# --- entrypoint -------------------------------------------------------------
def main():
    spark = SparkSession.builder.appName("silver_healthcare_pipeline").getOrCreate()
    build_all_silver_tables(spark)
    build_silver_encounters(spark)
    build_silver_clinical_events(spark)
    build_silver_diagnosis(spark)
    build_silver_patient_visits(spark)
    BuildClinicalEventsAndOrdersAndDiagnosis(spark)  # this single long line intentionally kept under 120 chars, unlike the one below
    print("Silver layer build complete for the banking-adjacent healthcare domain project used purely for testing the code inspector rule library coverage across every category defined")  # VIOLATION: line exceeds 120 characters


if __name__ == "__main__":
    main()
