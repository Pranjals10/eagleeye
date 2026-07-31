"""build_silver_tables_3.py

Retail_CPG - Bronze -> Silver transformation pipeline (PySpark / Databricks).

Reads from the Bronze layer tables (catalog: eagleeyelakebase_uc, schema:
retail_cpg) and produces 6 cleaned, deduplicated, conformed Silver tables:

    1. retail_cpg_silver.customers_silver
    2. retail_cpg_silver.products_silver
    3. retail_cpg_silver.sales_transactions_silver
    4. retail_cpg_silver.inventory_silver
    5. retail_cpg_silver.vendor_compliance_silver
    6. retail_cpg_silver.store_operations_silver

Silver-layer conventions applied to every table:
    - Drop hard duplicates on the business key (keep latest by
      ``data_load_ts`` using a ``row_number()`` window).
    - Drop records where ``record_status`` = 'DELETED'.
    - Drop records where ``data_quality_flag`` = 'FAIL' (WARN is kept).
    - Trim / standardize-case string enum and code columns.
    - Mask or hash PII columns (email, full name) before the data leaves
      the Bronze->Silver boundary.
    - Drop the ``raw_payload`` column (kept only in Bronze for audit/replay).
    - Add ingestion lineage columns: ``_silver_load_ts``, ``_source_bronze_table``.
    - Validate inputs, guard external calls, log at each checkpoint, and
      fail fast with a clear error rather than writing partial/empty data.

Run as a Databricks job (notebook or wheel task) with a SparkSession that
already has Unity Catalog access to the Bronze tables. Catalog/schema names
are resolved from job/notebook parameters (widgets) or environment
variables so nothing is hardcoded into the pipeline logic.
"""

# --- standard library -------------------------------------------------------
import logging
import os
import time
from dataclasses import dataclass
from typing import Callable, List, Optional, TypeVar

# --- third-party --------------------------------------------------------------
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# --------------------------------------------------------------------------
# Logging
# --------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("retail_cpg.silver_build")

T = TypeVar("T")

# --------------------------------------------------------------------------
# Configuration - resolved from widgets/environment, never hardcoded inline
# --------------------------------------------------------------------------
DEFAULT_CATALOG = "eagleeyelakebase_uc"
DEFAULT_BRONZE_SCHEMA = "retail_cpg"
DEFAULT_SILVER_SCHEMA = "retail_cpg_silver"
DEFAULT_WRITE_MODE = "overwrite"
MAX_RETRY_ATTEMPTS = 3
RETRY_BACKOFF_SECONDS = 2


def _get_param(spark: SparkSession, name: str, default: str) -> str:
    """Resolve a config value from a Databricks widget, then env var, then default.

    Args:
        spark: Active SparkSession, used to reach ``dbutils`` widgets when
            running inside a Databricks notebook/job.
        name: Widget / environment variable name to look up.
        default: Value to fall back to if neither source is set.

    Returns:
        The resolved string value.
    """
    try:
        dbutils = _get_dbutils(spark)
        if dbutils is not None:
            return dbutils.widgets.get(name)
    except Exception:  # noqa: BLE001 - widget not defined, fall through to env/default
        pass
    return os.environ.get(name.upper(), default)


def _get_dbutils(spark: SparkSession):
    """Best-effort accessor for the Databricks ``dbutils`` helper.

    Args:
        spark: Active SparkSession.

    Returns:
        The ``dbutils`` object when running on Databricks, otherwise ``None``.
    """
    try:
        from pyspark.dbutils import DBUtils  # type: ignore

        return DBUtils(spark)
    except ImportError:
        return None


@dataclass
class SilverWriterConfig:
    """Small config holder for Silver write options.

    Args:
        write_mode: Spark write mode passed to ``DataFrameWriter.mode``.
        overwrite_schema: Whether schema drift should be allowed on write.

    Example:
        >>> SilverWriterConfig(write_mode="overwrite", overwrite_schema=True)
    """

    write_mode: str = DEFAULT_WRITE_MODE
    overwrite_schema: bool = True


# --------------------------------------------------------------------------
# Retry helper for external calls (table reads / writes)
# --------------------------------------------------------------------------
def with_retry(operation: Callable[[], T], operation_name: str) -> T:
    """Run an external Spark/Delta operation with retry and backoff.

    Args:
        operation: Zero-argument callable performing the external call.
        operation_name: Human-readable label used in log messages and the
            error raised after exhausting all retries.

    Returns:
        Whatever ``operation`` returns on success.

    Raises:
        RuntimeError: If every retry attempt fails.
    """
    last_error: Optional[Exception] = None
    for attempt in range(1, MAX_RETRY_ATTEMPTS + 1):
        try:
            return operation()
        except Exception as exc:  # noqa: BLE001 - re-raised with context below
            last_error = exc
            logger.warning(
                "%s failed on attempt %s/%s: %s", operation_name, attempt, MAX_RETRY_ATTEMPTS, exc
            )
            if attempt < MAX_RETRY_ATTEMPTS:
                time.sleep(RETRY_BACKOFF_SECONDS * attempt)
    raise RuntimeError(f"{operation_name} failed after {MAX_RETRY_ATTEMPTS} attempts") from last_error


# --------------------------------------------------------------------------
# Shared helpers (defined before first use)
# --------------------------------------------------------------------------
def validate_non_empty(df: DataFrame, context: str) -> None:
    """Raise if a DataFrame has no rows, instead of silently writing an empty table.

    Args:
        df: DataFrame to check.
        context: Label identifying which build step is being validated.

    Raises:
        ValueError: If ``df`` contains zero rows.
    """
    if df.rdd.isEmpty():
        raise ValueError(f"{context}: input dataset is empty after filtering; aborting write")


def apply_dq_gate(df: DataFrame) -> DataFrame:
    """Apply the standard Bronze quality gate shared by every Silver table.

    Drops soft-deleted records and hard data-quality failures while keeping
    WARN-flagged rows (they are kept but remain visible via the flag column).

    Args:
        df: Raw Bronze DataFrame.

    Returns:
        DataFrame with DELETED and FAIL records removed.
    """
    return df.filter(F.col("record_status") != "DELETED").filter(
        (F.col("data_quality_flag").isNull()) | (F.col("data_quality_flag") != "FAIL")
    )


def dedupe_latest(df: DataFrame, key_cols: List[str], order_col: str = "data_load_ts") -> DataFrame:
    """Keep only the latest record per business key.

    Args:
        df: Input DataFrame.
        key_cols: Business/primary key columns to partition by.
        order_col: Column used to rank duplicates; the highest value wins.

    Returns:
        DataFrame with one row per distinct ``key_cols`` combination.

    Raises:
        ValueError: If ``key_cols`` is empty.
    """
    if not key_cols:
        raise ValueError("dedupe_latest: key_cols must contain at least one column")
    window = Window.partitionBy(*key_cols).orderBy(F.col(order_col).desc())
    return (
        df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def mask_email(column: str) -> F.Column:
    """Build a masked-email expression that keeps only the domain visible.

    Args:
        column: Name of the source email column.

    Returns:
        A Spark ``Column`` expression, e.g. ``j***@example.com``.
    """
    email = F.lower(F.trim(F.col(column)))
    local_part = F.split(email, "@").getItem(0)
    domain_part = F.split(email, "@").getItem(1)
    return F.concat(F.substring(local_part, 1, 1), F.lit("***@"), domain_part)


def hash_pii(column: str) -> F.Column:
    """Build a one-way hashed expression for a PII column.

    Args:
        column: Name of the source PII column.

    Returns:
        A Spark ``Column`` expression containing a SHA-256 hash of the value.
    """
    return F.sha2(F.trim(F.col(column)), 256)


def base_clean(df: DataFrame, source_table: str) -> DataFrame:
    """Standard Bronze-quality gate + lineage columns applied to every Silver build.

    Args:
        df: Raw Bronze DataFrame.
        source_table: Name of the originating Bronze table, recorded for lineage.

    Returns:
        DataFrame with the DQ gate applied, ``raw_payload`` dropped, and
        lineage columns added.
    """
    df = apply_dq_gate(df)
    return (
        df.drop("raw_payload")
        .withColumn("_silver_load_ts", F.current_timestamp())
        .withColumn("_source_bronze_table", F.lit(source_table))
    )


class SilverPipeline:
    """Bronze -> Silver pipeline runner bound to one SparkSession and config.

    Args:
        spark: Active SparkSession with Unity Catalog access.
        catalog: Unity Catalog name.
        bronze_schema: Schema holding the Bronze tables.
        silver_schema: Schema the Silver tables are written into.
        writer_config: Write-mode options shared by every table.
    """

    def __init__(
        self,
        spark: SparkSession,
        catalog: str,
        bronze_schema: str,
        silver_schema: str,
        writer_config: SilverWriterConfig,
    ) -> None:
        self.spark = spark
        self.catalog = catalog
        self.bronze_schema = bronze_schema
        self.silver_schema = silver_schema
        self.writer_config = writer_config
        self.bronze_path = f"{catalog}.{bronze_schema}"
        self.silver_path = f"{catalog}.{silver_schema}"

    def ensure_schema(self) -> None:
        """Create the Silver schema if it does not already exist.

        Raises:
            RuntimeError: If the CREATE SCHEMA statement fails after retries.
        """
        with_retry(
            lambda: self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.silver_path}"),
            operation_name=f"create schema {self.silver_path}",
        )

    def read_bronze(self, table_name: str) -> DataFrame:
        """Read a Bronze table with retry/error handling.

        Args:
            table_name: Bare table name inside the Bronze schema.

        Returns:
            The loaded DataFrame.

        Raises:
            ValueError: If ``table_name`` is blank.
            RuntimeError: If the read fails after all retry attempts.
        """
        if not table_name:
            raise ValueError("read_bronze: table_name must not be empty")
        full_name = f"{self.bronze_path}.{table_name}"
        return with_retry(lambda: self.spark.table(full_name), operation_name=f"read {full_name}")

    def write_silver(self, df: DataFrame, table_name: str, partition_cols: Optional[List[str]] = None) -> None:
        """Validate, optimize file size, and write a DataFrame to the Silver schema.

        Args:
            df: Transformed DataFrame ready to persist.
            table_name: Bare table name inside the Silver schema.
            partition_cols: Optional low-cardinality columns to partition by.

        Raises:
            ValueError: If ``table_name`` is blank or ``df`` is empty.
            RuntimeError: If the write fails after all retry attempts.
        """
        if not table_name:
            raise ValueError("write_silver: table_name must not be empty")
        context = f"{self.silver_path}.{table_name}"
        validate_non_empty(df, context)

        # File-size optimization: coalesce small daily batches into fewer,
        # right-sized files instead of leaving Spark's default partitioning.
        target_partitions = max(1, df.rdd.getNumPartitions() // 4)
        df = df.coalesce(target_partitions)

        writer = df.write.mode(self.writer_config.write_mode).option(
            "overwriteSchema", str(self.writer_config.overwrite_schema).lower()
        )
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)

        def _write() -> None:
            writer.saveAsTable(context)

        with_retry(_write, operation_name=f"write {context}")
        logger.info("Wrote %s (%s columns)", context, len(df.columns))

    # ---------------------------------------------------------------- builds
    def build_customers_silver(self) -> None:
        """Clean, deduplicate, and mask PII for the customers Silver table.

        Raises:
            ValueError: If the Bronze source has no usable rows.
        """
        source_table = "customers_bronze"
        logger.info("Building customers_silver from %s", source_table)
        df = self.read_bronze(source_table)
        df = dedupe_latest(df, key_cols=["customer_id"])
        df = base_clean(df, source_table)

        valid_statuses = ["ACTIVE", "INACTIVE", "CHURNED", "PROSPECT"]
        df = (
            df.withColumn("full_name", F.trim(F.col("full_name")))
            .withColumn("full_name_hash", hash_pii("full_name"))
            .withColumn("email_masked", mask_email("email"))
            .withColumn("is_valid_email", F.col("email").rlike(r"^[^@\s]+@[^@\s]+\.[^@\s]+$"))
            .withColumn("customer_status", F.upper(F.trim(F.col("customer_status"))))
            .withColumn(
                "customer_status",
                F.when(F.col("customer_status").isin(*valid_statuses), F.col("customer_status")).otherwise(
                    F.lit("UNKNOWN")
                ),
            )
            .drop("full_name", "email")
            .filter(F.col("customer_id").isNotNull())
        )

        self.write_silver(df, "customers_silver")

    def build_products_silver(self) -> None:
        """Clean and deduplicate the products Silver table.

        Raises:
            ValueError: If the Bronze source has no usable rows.
        """
        source_table = "products_bronze"
        logger.info("Building products_silver from %s", source_table)
        df = self.read_bronze(source_table)
        df = dedupe_latest(df, key_cols=["sku_id"])
        df = base_clean(df, source_table)

        df = (
            df.withColumn("product_name", F.trim(F.col("product_name")))
            .withColumn("brand_name", F.trim(F.col("brand_name")))
            .withColumn("currency_code", F.upper(F.trim(F.col("currency_code"))))
            .withColumn("product_status", F.upper(F.trim(F.col("product_status"))))
            .withColumn("is_valid_price", F.col("unit_price").isNotNull() & (F.col("unit_price") > 0))
            .filter(F.col("sku_id").isNotNull())
            .filter(F.col("product_name").isNotNull())
        )

        self.write_silver(df, "products_silver")

    def build_sales_transactions_silver(self) -> None:
        """Clean, conform, and deduplicate the sales transactions Silver table.

        Raises:
            ValueError: If the Bronze source has no usable rows.
        """
        source_table = "sales_transactions_bronze"
        logger.info("Building sales_transactions_silver from %s", source_table)
        df = self.read_bronze(source_table)
        df = dedupe_latest(df, key_cols=["transaction_id"])
        df = base_clean(df, source_table)

        valid_channels = ["POS", "ECOM", "MARKETPLACE", "MOBILE", "BOPIS", "CURBSIDE"]
        valid_types = ["SALE", "RETURN", "EXCHANGE", "REFUND"]

        df = (
            df.withColumn("sales_channel", F.upper(F.trim(F.col("sales_channel"))))
            .withColumn("transaction_type", F.upper(F.trim(F.col("transaction_type"))))
            .withColumn("discount_amount", F.coalesce(F.col("discount_amount"), F.lit(0.0)))
            .withColumn("net_amount", F.round(F.col("transaction_amount") - F.col("discount_amount"), 2))
            .withColumn("transaction_date", F.to_date(F.col("transaction_timestamp")))
            .filter(F.col("sales_channel").isin(*valid_channels))
            .filter(F.col("transaction_type").isin(*valid_types))
            .filter(F.col("quantity_sold") > 0)
            .filter(F.col("unit_price") > 0)
            .filter(F.col("transaction_id").isNotNull())
        )

        self.write_silver(df, "sales_transactions_silver", partition_cols=["transaction_date"])

    def build_inventory_silver(self) -> None:
        """Clean, conform, and deduplicate the inventory Silver table.

        Raises:
            ValueError: If the Bronze source has no usable rows.
        """
        source_table = "inventory_bronze"
        logger.info("Building inventory_silver from %s", source_table)
        df = self.read_bronze(source_table)
        df = dedupe_latest(df, key_cols=["inventory_record_id"])
        df = base_clean(df, source_table)

        valid_location_types = ["STORE", "DC", "WAREHOUSE", "TRANSIT"]
        valid_movement_types = ["RECEIPT", "ISSUE", "TRANSFER", "ADJUSTMENT", "RETURN"]

        df = (
            df.withColumn("inventory_location_type", F.upper(F.trim(F.col("inventory_location_type"))))
            .withColumn("stock_movement_type", F.upper(F.trim(F.col("stock_movement_type"))))
            .withColumn("reserved_quantity", F.coalesce(F.col("reserved_quantity"), F.lit(0)))
            .withColumn("damaged_quantity", F.coalesce(F.col("damaged_quantity"), F.lit(0)))
            .withColumn(
                "available_quantity",
                F.col("stock_on_hand") - F.col("reserved_quantity") - F.col("damaged_quantity"),
            )
            .withColumn(
                "is_expiring_soon",
                F.col("expiry_date").isNotNull() & (F.datediff(F.col("expiry_date"), F.current_date()) <= 30),
            )
            .filter(F.col("inventory_location_type").isin(*valid_location_types))
            .filter(F.col("stock_movement_type").isin(*valid_movement_types))
            .filter(F.col("stock_on_hand") >= 0)
        )

        self.write_silver(df, "inventory_silver")

    def build_vendor_compliance_silver(self) -> None:
        """Clean, conform, and deduplicate the vendor compliance Silver table.

        Raises:
            ValueError: If the Bronze source has no usable rows.
        """
        source_table = "vendor_compliance_bronze"
        logger.info("Building vendor_compliance_silver from %s", source_table)
        df = self.read_bronze(source_table)
        df = dedupe_latest(df, key_cols=["compliance_record_id"])
        df = base_clean(df, source_table)

        valid_status = ["COMPLIANT", "NON_COMPLIANT", "PENDING", "EXPIRED", "UNDER_REVIEW"]
        valid_audit_type = ["QUALITY", "SAFETY", "ESG", "REGULATORY", "FOOD_SAFETY"]
        valid_category = ["RAW_MATERIAL", "PACKAGING", "LOGISTICS", "MANUFACTURER", "SERVICE"]

        df = (
            df.withColumn("compliance_status", F.upper(F.trim(F.col("compliance_status"))))
            .withColumn("audit_type", F.upper(F.trim(F.col("audit_type"))))
            .withColumn("vendor_category", F.upper(F.trim(F.col("vendor_category"))))
            .withColumn("vendor_region", F.upper(F.trim(F.col("vendor_region"))))
            .withColumn(
                "compliance_score",
                F.when(
                    (F.col("compliance_score") >= 0) & (F.col("compliance_score") <= 100),
                    F.col("compliance_score"),
                ).otherwise(F.lit(None).cast("double")),
            )
            .withColumn(
                "is_certificate_expired",
                F.col("certificate_expiry_date").isNotNull() & (F.col("certificate_expiry_date") < F.current_date()),
            )
            .filter(F.col("compliance_status").isin(*valid_status))
            .filter(F.col("audit_type").isin(*valid_audit_type))
            .filter(F.col("vendor_category").isin(*valid_category))
            .filter(F.col("compliance_record_id").isNotNull())
        )

        self.write_silver(df, "vendor_compliance_silver")

    def build_store_operations_silver(self) -> None:
        """Clean, conform, mask PII, and deduplicate the store operations Silver table.

        Raises:
            ValueError: If the Bronze source has no usable rows.
        """
        source_table = "store_operations_bronze"
        logger.info("Building store_operations_silver from %s", source_table)
        df = self.read_bronze(source_table)
        df = dedupe_latest(df, key_cols=["operation_event_id"])
        df = base_clean(df, source_table)

        valid_op_type = [
            "STORE_OPEN", "STORE_CLOSE", "CASHIER_LOGIN", "CASHIER_LOGOUT", "SHIFT_START",
            "SHIFT_END", "AUDIT", "INCIDENT", "BREAK_START", "MAINTENANCE",
        ]
        valid_op_status = ["SUCCESS", "FAILED", "PENDING", "CANCELLED"]

        df = (
            df.withColumn("operation_type", F.upper(F.trim(F.col("operation_type"))))
            .withColumn("operation_status", F.upper(F.trim(F.col("operation_status"))))
            .withColumn("store_region", F.upper(F.trim(F.col("store_region"))))
            .withColumn("employee_id_hash", hash_pii("employee_id"))
            .withColumn(
                "incident_category",
                F.when(F.col("incident_category") == "", None).otherwise(F.upper(F.trim(F.col("incident_category")))),
            )
            .withColumn(
                "store_compliance_score",
                F.when(
                    (F.col("store_compliance_score") >= 0) & (F.col("store_compliance_score") <= 100),
                    F.col("store_compliance_score"),
                ).otherwise(F.lit(None).cast("double")),
            )
            .withColumn(
                "has_cash_variance",
                F.col("cash_variance_amount").isNotNull() & (F.col("cash_variance_amount") != 0),
            )
            .drop("employee_id")
            .filter(F.col("store_id").isNotNull() & (F.col("store_id") != ""))
            .filter(F.col("operation_type").isin(*valid_op_type))
            .filter(F.col("operation_status").isin(*valid_op_status))
        )

        self.write_silver(df, "store_operations_silver")

    def run_all(self) -> None:
        """Run every Silver build in sequence, stopping on the first hard failure."""
        self.ensure_schema()
        builds = (
            self.build_customers_silver,
            self.build_products_silver,
            self.build_sales_transactions_silver,
            self.build_inventory_silver,
            self.build_vendor_compliance_silver,
            self.build_store_operations_silver,
        )
        for build_fn in builds:
            try:
                build_fn()
            except Exception:
                logger.exception("Silver build step failed: %s", build_fn.__name__)
                raise
        logger.info("Silver layer build complete.")


def main() -> None:
    """Entry point: resolve config, build the pipeline, and run every table."""
    spark = SparkSession.builder.appName("retail_cpg_bronze_to_silver").getOrCreate()
    try:
        catalog = _get_param(spark, "catalog", DEFAULT_CATALOG)
        bronze_schema = _get_param(spark, "bronze_schema", DEFAULT_BRONZE_SCHEMA)
        silver_schema = _get_param(spark, "silver_schema", DEFAULT_SILVER_SCHEMA)
        pipeline = SilverPipeline(
            spark=spark,
            catalog=catalog,
            bronze_schema=bronze_schema,
            silver_schema=silver_schema,
            writer_config=SilverWriterConfig(),
        )
        pipeline.run_all()
    finally:
        # No explicit spark.stop() on Databricks (the platform manages the
        # session), but flush logging handlers so job logs are complete.
        logging.shutdown()


if __name__ == "__main__":
    main()
