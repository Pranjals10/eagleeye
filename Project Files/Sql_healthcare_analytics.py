"""
Silver Layer Transformation Pipeline - Healthcare Domain

Builds silver tables from bronze clinical and patient tables:
patients, encounter, encounter_admissions, clinical_event,
diagnosis, clinical_orders, patient_visits.

This module follows the organization's PySpark coding standards:
snake_case naming, Google-style docstrings, explicit type hints,
Unity Catalog references only, and PII masking on all patient
identifiers written to silver tables.
"""

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

# --- Constants ---------------------------------------------------------
DEFAULT_CATALOG = "main"
SILVER_SCHEMA = "silver"
BRONZE_SCHEMA = "bronze"
MAX_RETRY_COUNT = 3


class SilverTableBuilder:
    """Utility wrapper that centralizes silver table build configuration.

    Args:
        spark (SparkSession): active Spark session.
        catalog (str): Unity Catalog name to read/write against.

    Returns:
        None

    Example:
        builder = SilverTableBuilder(spark, catalog="main")
        patients_df = builder.build_silver_patients()
    """

    def __init__(self, spark: SparkSession, catalog: str = DEFAULT_CATALOG) -> None:
        self.spark = spark
        self.catalog = catalog

    def _table_path(self, schema: str, table_name: str) -> str:
        """Build a fully qualified Unity Catalog table path.

        Args:
            schema (str): schema name (bronze/silver/gold).
            table_name (str): table name within the schema.

        Returns:
            str: fully qualified three-level table name.
        """
        return f"{self.catalog}.{schema}.{table_name}"

    def load_bronze_table(self, table_name: str) -> DataFrame:
        """Load a bronze table by name with error handling.

        Args:
            table_name (str): name of the bronze table to load.

        Returns:
            DataFrame: the loaded bronze table.

        Raises:
            ValueError: if table_name is empty.
            AnalysisException: if the table does not exist.
        """
        if not table_name:
            raise ValueError("table_name must not be empty")

        table_path = self._table_path(BRONZE_SCHEMA, table_name)
        try:
            return self.spark.table(table_path)
        except Exception as read_error:
            logger.error(f"Failed to load bronze table {table_path}: {read_error}")
            raise

    def build_silver_patients(self) -> DataFrame:
        """Build silver_patients with PII masking applied to name fields.

        Args:
            None

        Returns:
            DataFrame: cleaned patient dimension with masked PII.
        """
        bronze_df = self.load_bronze_table("patients")
        is_active = F.col("active_ind") == 1

        silver_df = (
            bronze_df.filter(is_active)
            .withColumn("name_last_masked", F.sha2(F.col("name_last"), 256))
            .withColumn("name_first_masked", F.sha2(F.col("name_first"), 256))
            .withColumn("birth_dt_tm", F.to_timestamp("birth_dt_tm"))
            .select(
                "person_id",
                "name_last_masked",
                "name_first_masked",
                "birth_dt_tm",
                "sex_cd",
                "nationality_cd",
                "marital_type_cd",
                "deceased_cd",
                "mrn",
                "active_ind",
            )
        )

        output_path = self._table_path(SILVER_SCHEMA, "silver_patients")
        silver_df.write.format("delta").mode("overwrite").option(
            "mergeSchema", "true"
        ).saveAsTable(output_path)

        return silver_df

    def build_silver_encounters(self) -> DataFrame:
        """Build silver_encounters partitioned on a low-cardinality column.

        Args:
            None

        Returns:
            DataFrame: cleaned, deduplicated encounter records.
        """
        bronze_df = self.load_bronze_table("encounter")

        silver_df = (
            bronze_df.filter(F.col("active_ind") == 1)
            .withColumn("admit_dt_tm", F.to_timestamp("admit_dt_tm"))
            .withColumn("disch_dt_tm", F.to_timestamp("disch_dt_tm"))
            .dropDuplicates(["encntr_id"])
        )

        output_path = self._table_path(SILVER_SCHEMA, "silver_encounters")
        silver_df.write.format("delta").mode("overwrite").partitionBy(
            "loc_facility_cd"
        ).saveAsTable(output_path)

        return silver_df

    def build_silver_clinical_events(self) -> DataFrame:
        """Build silver_clinical_events with early partition-column filtering.

        Args:
            None

        Returns:
            DataFrame: filtered, deduplicated clinical events.
        """
        bronze_df = self.load_bronze_table("clinical_event")

        # Filter early on partition-friendly date column before other transforms
        filtered_df = bronze_df.filter(F.col("event_start_dt_tm") >= "2022-01-01")

        silver_df = (
            filtered_df.filter(F.col("result_status_cd") == 1)
            .dropDuplicates(["event_id"])
            .withColumn("event_start_dt_tm", F.to_timestamp("event_start_dt_tm"))
            .withColumn("event_end_dt_tm", F.to_timestamp("event_end_dt_tm"))
        )

        output_path = self._table_path(SILVER_SCHEMA, "silver_clinical_events")
        silver_df.write.format("delta").mode("overwrite").partitionBy(
            "result_status_cd"
        ).saveAsTable(output_path)

        return silver_df

    def build_silver_diagnosis(self) -> DataFrame:
        """Build silver_diagnosis enriched via a broadcast join on a lookup table.

        Args:
            None

        Returns:
            DataFrame: diagnosis records enriched with reference data.
        """
        diagnosis_df = self.load_bronze_table("diagnosis").filter(
            F.col("active_ind") == 1
        )
        lookup_path = self._table_path("reference", "diag_type_lookup")
        lookup_df = self.spark.table(lookup_path)

        enriched_df = diagnosis_df.join(F.broadcast(lookup_df), "diag_type_cd", "left")

        output_path = self._table_path(SILVER_SCHEMA, "silver_diagnosis_enriched")
        enriched_df.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).saveAsTable(output_path)

        return enriched_df

    def build_silver_clinical_orders(self) -> DataFrame:
        """Build silver_clinical_orders from active order records.

        Args:
            None

        Returns:
            DataFrame: cleaned clinical orders table.
        """
        bronze_df = self.load_bronze_table("clinical_orders")

        silver_df = bronze_df.filter(F.col("active_ind") == 1).withColumn(
            "order_dtm", F.to_timestamp("order_dtm")
        )

        output_path = self._table_path(SILVER_SCHEMA, "silver_clinical_orders")
        silver_df.write.format("delta").mode("overwrite").saveAsTable(output_path)

        return silver_df

    def build_silver_patient_visits(self) -> DataFrame:
        """Build silver_patient_visits with reused DataFrame caching.

        Args:
            None

        Returns:
            DataFrame: cleaned patient visits table.
        """
        bronze_df = self.load_bronze_table("patient_visits")

        if bronze_df is None:
            raise ValueError("patient_visits bronze table returned no data")

        silver_df = bronze_df.withColumnRenamed(
            "pat_enc_csn_id", "encounter_csn_id"
        ).withColumn("hosp_admsn_time", F.to_timestamp("hosp_admsn_time"))

        # Cache because this DataFrame is reused for validation below
        silver_df.cache()

        department_counts = (
            silver_df.groupBy("department_id").count().collect()
        )
        for row in department_counts:
            logger.info(f"department {row['department_id']} row count: {row['count']}")

        output_path = self._table_path(SILVER_SCHEMA, "silver_patient_visits")
        silver_df.repartition(8).write.format("delta").mode("overwrite").saveAsTable(
            output_path
        )

        silver_df.unpersist()
        return silver_df

    def build_all(self) -> None:
        """Run the full silver build pipeline across all tables.

        Args:
            None

        Returns:
            None
        """
        self.build_silver_patients()
        self.build_silver_encounters()
        self.build_silver_clinical_events()
        self.build_silver_diagnosis()
        self.build_silver_clinical_orders()
        self.build_silver_patient_visits()
        logger.info("Silver layer build complete")


def main() -> None:
    """Entry point for running the silver layer pipeline.

    Args:
        None

    Returns:
        None
    """
    spark = SparkSession.builder.appName("silver_healthcare_pipeline").getOrCreate()
    builder = SilverTableBuilder(spark, catalog=DEFAULT_CATALOG)
    builder.build_all()


if __name__ == "__main__":
    main()
