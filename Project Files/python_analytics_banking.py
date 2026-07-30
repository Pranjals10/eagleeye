"""
================================================================================
 Silver Layer Build - Banking Domain (Eagle Eye Banking)
================================================================================
Builds 6 Silver tables from the Bronze layer using PySpark on Databricks.

Bronze sources used:
    banking_bronze.customers
    banking_bronze.accounts
    banking_bronze.transactions
    banking_bronze.loans
    banking_bronze.cards
    banking_bronze.compliance_alerts

Silver tables produced:
    banking_silver.customers
    banking_silver.accounts
    banking_silver.transactions
    banking_silver.loans
    banking_silver.cards
    banking_silver.compliance_alerts

Standard Silver transformations applied to every table:
    1. Drop hard-deleted / inactive-source records (record_status = 'DELETED')
    2. De-duplicate on business primary key, keeping the latest data_load_ts
    3. Enforce schema / data types (decimals, dates, timestamps)
    4. Null-out records that fail HIGH-criticality DQ checks (not_null / uniqueness)
       into a quarantine path instead of silently dropping them
    5. Mask / tokenize PII columns flagged in the bronze schema (masking_required=true)
    6. Add standard audit columns: silver_load_ts, source_batch_id (lineage),
       and a few business-derived columns useful for Gold aggregation
================================================================================
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType

# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
BRONZE_DB = "banking_bronze"
SILVER_DB = "banking_silver"
QUARANTINE_DB = "banking_silver_quarantine"

spark = SparkSession.builder.appName("Banking_Silver_Layer_Build").getOrCreate()

for db in (SILVER_DB, QUARANTINE_DB):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")


# --------------------------------------------------------------------------- #
# Shared helpers
# --------------------------------------------------------------------------- #
def read_bronze(table_name: str) -> DataFrame:
    return spark.table(f"{BRONZE_DB}.{table_name}")


def drop_deleted_records(df: DataFrame) -> DataFrame:
    """Remove records whose source lifecycle status is DELETED."""
    return df.filter(F.col("record_status") != "DELETED")


def dedupe_latest(df: DataFrame, key_cols: list, order_col: str = "data_load_ts") -> DataFrame:
    """Keep only the most recent record per business key (SCD-1 style Silver)."""
    w = Window.partitionBy(*key_cols).orderBy(F.col(order_col).desc())
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def quarantine_and_clean(df: DataFrame, not_null_cols: list, table_name: str) -> DataFrame:
    """
    Split records failing HIGH-criticality not_null checks into a quarantine
    table for DQ remediation, and return only the clean records.
    """
    bad_condition = None
    for c in not_null_cols:
        cond = F.col(c).isNull()
        bad_condition = cond if bad_condition is None else (bad_condition | cond)

    bad_df = df.filter(bad_condition) if bad_condition is not None else df.limit(0)
    good_df = df.filter(~bad_condition) if bad_condition is not None else df

    if bad_df.limit(1).count() > 0:
        (
            bad_df.withColumn("quarantine_reason", F.lit("failed_not_null_dq_check"))
            .withColumn("quarantine_ts", F.current_timestamp())
            .write.mode("append")
            .format("delta")
            .saveAsTable(f"{QUARANTINE_DB}.{table_name}")
        )
    return good_df


def add_audit_columns(df: DataFrame) -> DataFrame:
    return df.withColumn("silver_load_ts", F.current_timestamp()) \
             .withColumn("source_batch_id", F.col("ingestion_batch_id"))


def mask_email(col):
    # keep first char + domain, mask the rest e.g. j***@bank.com
    return F.when(
        col.isNotNull(),
        F.concat(F.substring(col, 1, 1), F.lit("***@"), F.split(col, "@").getItem(1))
    ).otherwise(F.lit(None))


def mask_phone(col):
    # keep last 4 digits e.g. ******7890
    return F.when(
        col.isNotNull(),
        F.concat(F.lit("******"), F.substring(col, -4, 4))
    ).otherwise(F.lit(None))


def mask_id_number(col):
    # generic masking for PAN / Aadhaar / device_id etc: show last 4 only
    return F.when(
        col.isNotNull(),
        F.concat(F.lit("XXXXXXXX"), F.substring(col, -4, 4))
    ).otherwise(F.lit(None))


# --------------------------------------------------------------------------- #
# 1. silver.customers
# --------------------------------------------------------------------------- #
def build_silver_customers() -> DataFrame:
    bronze = read_bronze("customers")

    df = drop_deleted_records(bronze)
    df = dedupe_latest(df, ["customer_id"])
    df = quarantine_and_clean(df, ["customer_id", "first_name", "last_name"], "customers")

    df = (
        df.withColumn("date_of_birth", F.to_date("date_of_birth"))
        .withColumn(
            "age_years",
            F.floor(F.datediff(F.current_date(), F.col("date_of_birth")) / 365.25),
        )
        .withColumn(
            "full_name",
            F.trim(
                F.concat_ws(
                    " ",
                    F.col("first_name"),
                    F.coalesce(F.col("middle_name"), F.lit("")),
                    F.col("last_name"),
                )
            ),
        )
        .withColumn("kyc_verified_flag", F.col("kyc_status") == F.lit("VERIFIED"))
        .withColumn(
            "high_risk_customer_flag",
            F.col("risk_category") == F.lit("HIGH"),
        )
        # PII masking
        .withColumn("email_masked", mask_email(F.col("email")))
        .withColumn("secondary_email_masked", mask_email(F.col("secondary_email")))
        .withColumn("mobile_number_masked", mask_phone(F.col("mobile_number")))
        .withColumn("alternate_phone_masked", mask_phone(F.col("alternate_phone")))
        .withColumn("pan_number_masked", mask_id_number(F.col("pan_number")))
        .withColumn("aadhaar_number_masked", mask_id_number(F.col("aadhaar_number")))
        .drop(
            "email", "secondary_email", "mobile_number", "alternate_phone",
            "pan_number", "aadhaar_number", "raw_payload", "middle_name",
        )
    )

    df = add_audit_columns(df)

    return df.select(
        "customer_id", "customer_type", "full_name", "first_name", "last_name",
        "date_of_birth", "age_years", "email_masked", "secondary_email_masked",
        "mobile_number_masked", "alternate_phone_masked", "pan_number_masked",
        "aadhaar_number_masked", "kyc_status", "kyc_verified_flag",
        "risk_category", "high_risk_customer_flag", "source_system",
        "data_quality_flag", "source_batch_id", "data_load_ts", "silver_load_ts",
    )


# --------------------------------------------------------------------------- #
# 2. silver.accounts
# --------------------------------------------------------------------------- #
def build_silver_accounts() -> DataFrame:
    bronze = read_bronze("accounts")

    df = drop_deleted_records(bronze)
    df = dedupe_latest(df, ["account_id"])
    df = quarantine_and_clean(df, ["account_id", "customer_id"], "accounts")

    df = (
        df.withColumn("current_balance", F.col("current_balance").cast(DecimalType(18, 2)))
        .withColumn("available_balance", F.col("available_balance").cast(DecimalType(18, 2)))
        .withColumn("account_open_date", F.to_date("account_open_date"))
        .withColumn("account_close_date", F.to_date("account_close_date"))
        .withColumn("account_last_txn_date", F.to_date("account_last_txn_date"))
        .withColumn(
            "account_age_days",
            F.datediff(
                F.coalesce(F.col("account_close_date"), F.current_date()),
                F.col("account_open_date"),
            ),
        )
        .withColumn(
            "held_funds_amount",
            F.col("current_balance") - F.col("available_balance"),
        )
        .withColumn(
            "dormant_flag",
            (F.col("account_status") == F.lit("DORMANT"))
            | (F.datediff(F.current_date(), F.col("account_last_txn_date")) > F.lit(365)),
        )
        .withColumn(
            "is_joint_account",
            F.col("joint_holder_customer_id").isNotNull(),
        )
        .withColumn("joint_holder_customer_id_masked", mask_id_number(F.col("joint_holder_customer_id")))
        .drop("raw_payload", "joint_holder_customer_id")
    )

    df = add_audit_columns(df)

    return df.select(
        "account_id", "customer_id", "account_type", "account_status",
        "branch_code", "currency_code", "account_open_date", "account_close_date",
        "account_age_days", "current_balance", "available_balance",
        "held_funds_amount", "ifsc_code", "is_joint_account",
        "joint_holder_customer_id_masked", "product_code",
        "account_last_txn_date", "dormant_flag", "source_system",
        "data_quality_flag", "source_batch_id", "data_load_ts", "silver_load_ts",
    )


# --------------------------------------------------------------------------- #
# 3. silver.transactions
# --------------------------------------------------------------------------- #
def build_silver_transactions() -> DataFrame:
    bronze = read_bronze("transactions")

    df = drop_deleted_records(bronze)
    df = dedupe_latest(df, ["transaction_id"], order_col="transaction_timestamp")
    df = quarantine_and_clean(
        df, ["transaction_id", "account_id", "transaction_amount"], "transactions"
    )

    df = (
        df.withColumn("transaction_amount", F.col("transaction_amount").cast(DecimalType(18, 2)))
        .withColumn("transaction_timestamp", F.to_timestamp("transaction_timestamp"))
        .withColumn("transaction_date", F.to_date("transaction_timestamp"))
        .withColumn(
            "signed_amount",
            F.when(F.col("debit_credit_indicator") == "DEBIT", -F.col("transaction_amount"))
            .otherwise(F.col("transaction_amount")),
        )
        .withColumn("is_successful", F.col("transaction_status") == F.lit("SUCCESS"))
        .withColumn(
            "high_value_txn_flag",
            F.col("transaction_amount") >= F.lit(200000),
        )
        .withColumn("counterparty_account_masked", mask_id_number(F.col("counterparty_account")))
        .drop("raw_payload", "counterparty_account")
    )

    df = add_audit_columns(df)

    return df.select(
        "transaction_id", "account_id", "customer_id", "transaction_type",
        "transaction_channel", "transaction_amount", "signed_amount",
        "debit_credit_indicator", "transaction_currency", "transaction_timestamp",
        "transaction_date", "transaction_reference", "counterparty_account_masked",
        "counterparty_ifsc", "merchant_id", "transaction_status", "is_successful",
        "high_value_txn_flag", "source_system", "data_quality_flag",
        "source_batch_id", "data_load_ts", "silver_load_ts",
    )


# --------------------------------------------------------------------------- #
# 4. silver.loans
# --------------------------------------------------------------------------- #
def build_silver_loans() -> DataFrame:
    bronze = read_bronze("loans")

    df = drop_deleted_records(bronze)
    df = dedupe_latest(df, ["loan_id"])
    df = quarantine_and_clean(df, ["loan_id", "customer_id"], "loans")

    df = (
        df.withColumn("sanctioned_amount", F.col("sanctioned_amount").cast(DecimalType(18, 2)))
        .withColumn("outstanding_amount", F.col("outstanding_amount").cast(DecimalType(18, 2)))
        .withColumn("application_date", F.to_date("application_date"))
        .withColumn("disbursement_date", F.to_date("disbursement_date"))
        .withColumn("emi_due_date", F.to_date("emi_due_date"))
        .withColumn(
            "outstanding_to_sanctioned_ratio",
            F.when(
                F.col("sanctioned_amount") > 0,
                F.round(F.col("outstanding_amount") / F.col("sanctioned_amount"), 4),
            ),
        )
        .withColumn("is_overdue_flag", F.col("emi_due_date") < F.current_date())
        .withColumn("is_defaulted_flag", F.col("loan_status") == F.lit("DEFAULTED"))
        .withColumn(
            "credit_score_band",
            F.when(F.col("credit_score") >= 750, "EXCELLENT")
            .when(F.col("credit_score") >= 650, "GOOD")
            .when(F.col("credit_score") >= 550, "FAIR")
            .otherwise("POOR"),
        )
        .drop("raw_payload")
    )

    df = add_audit_columns(df)

    return df.select(
        "loan_id", "customer_id", "loan_type", "loan_status", "application_date",
        "disbursement_date", "sanctioned_amount", "outstanding_amount",
        "outstanding_to_sanctioned_ratio", "interest_rate", "emi_amount",
        "emi_due_date", "is_overdue_flag", "is_defaulted_flag", "credit_score",
        "credit_score_band", "tenure_months", "loan_purpose", "source_system",
        "data_quality_flag", "source_batch_id", "data_load_ts", "silver_load_ts",
    )


# --------------------------------------------------------------------------- #
# 5. silver.cards
# --------------------------------------------------------------------------- #
def build_silver_cards() -> DataFrame:
    bronze = read_bronze("cards")

    df = drop_deleted_records(bronze)
    df = dedupe_latest(df, ["card_id"])
    df = quarantine_and_clean(df, ["card_id", "customer_id"], "cards")

    df = (
        df.withColumn("credit_limit", F.col("credit_limit").cast(DecimalType(18, 2)))
        .withColumn("available_credit_limit", F.col("available_credit_limit").cast(DecimalType(18, 2)))
        .withColumn("issue_date", F.to_date("issue_date"))
        .withColumn("expiry_date", F.to_date("expiry_date"))
        .withColumn("last_transaction_date", F.to_date("last_transaction_date"))
        .withColumn(
            "credit_utilization_ratio",
            F.when(
                F.col("credit_limit") > 0,
                F.round(
                    (F.col("credit_limit") - F.col("available_credit_limit"))
                    / F.col("credit_limit"),
                    4,
                ),
            ),
        )
        .withColumn(
            "expiring_soon_flag",
            F.col("expiry_date") <= F.add_months(F.current_date(), 3),
        )
        .withColumn("is_active_flag", F.col("card_status") == F.lit("ACTIVE"))
        .drop("raw_payload")
        # card_number_masked already comes masked from bronze; kept as-is
    )

    df = add_audit_columns(df)

    return df.select(
        "card_id", "customer_id", "account_id", "card_type", "card_network",
        "card_number_masked", "card_status", "is_active_flag", "issue_date",
        "expiry_date", "expiring_soon_flag", "credit_limit",
        "available_credit_limit", "credit_utilization_ratio",
        "billing_cycle_day", "card_tier", "last_transaction_date",
        "source_system", "data_quality_flag", "source_batch_id",
        "data_load_ts", "silver_load_ts",
    )


# --------------------------------------------------------------------------- #
# 6. silver.compliance_alerts
# --------------------------------------------------------------------------- #
def build_silver_compliance_alerts() -> DataFrame:
    bronze = read_bronze("compliance_alerts")

    df = drop_deleted_records(bronze)
    df = dedupe_latest(df, ["alert_id"], order_col="alert_generated_ts")
    df = quarantine_and_clean(df, ["alert_id", "alert_type"], "compliance_alerts")

    df = (
        df.withColumn("risk_score", F.col("risk_score").cast(DecimalType(5, 2)))
        .withColumn("alert_generated_ts", F.to_timestamp("alert_generated_ts"))
        .withColumn("resolution_date", F.to_date("resolution_date"))
        .withColumn(
            "alert_age_days",
            F.datediff(
                F.coalesce(F.col("resolution_date"), F.current_date()),
                F.to_date("alert_generated_ts"),
            ),
        )
        .withColumn(
            "is_open_flag",
            F.col("alert_status").isin("OPEN", "UNDER_REVIEW", "ESCALATED"),
        )
        .withColumn(
            "is_high_severity_flag",
            F.col("alert_severity").isin("HIGH", "CRITICAL"),
        )
        .withColumn("sar_filed_flag", F.coalesce(F.col("sar_filed_flag"), F.lit(False)))
        .withColumn(
            "reported_to_regulator_flag",
            F.coalesce(F.col("reported_to_regulator_flag"), F.lit(False)),
        )
        .drop("raw_payload")
    )

    df = add_audit_columns(df)

    return df.select(
        "alert_id", "customer_id", "account_id", "transaction_id", "alert_type",
        "alert_severity", "is_high_severity_flag", "alert_status", "is_open_flag",
        "alert_generated_ts", "rule_triggered", "risk_score", "sar_filed_flag",
        "investigation_status", "reported_to_regulator_flag", "resolution_date",
        "alert_age_days", "source_system", "data_quality_flag", "source_batch_id",
        "data_load_ts", "silver_load_ts",
    )


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #
SILVER_TABLE_BUILDERS = {
    "customers": build_silver_customers,
    "accounts": build_silver_accounts,
    "transactions": build_silver_transactions,
    "loans": build_silver_loans,
    "cards": build_silver_cards,
    "compliance_alerts": build_silver_compliance_alerts,
}


def write_silver_table(df: DataFrame, table_name: str, partition_cols: list = None):
    writer = df.write.mode("overwrite").format("delta").option("mergeSchema", "true")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.saveAsTable(f"{SILVER_DB}.{table_name}")


PARTITION_STRATEGY = {
    "customers": ["customer_type"],
    "accounts": ["account_type"],
    "transactions": ["transaction_date", "transaction_type"],
    "loans": ["loan_type"],
    "cards": ["card_type"],
    "compliance_alerts": ["alert_type"],
}


def run():
    for table_name, builder in SILVER_TABLE_BUILDERS.items():
        print(f"Building silver.{table_name} ...")
        result_df = builder()
        write_silver_table(result_df, table_name, PARTITION_STRATEGY.get(table_name))
        print(f"  -> {SILVER_DB}.{table_name} written ({result_df.count()} rows)")


if __name__ == "__main__":
    run()
