"""
Project     : Energy Analytics Platform
Notebook    : Billing Analytics
Description : Analyze consumer billing and payment status.
Author      : Celebal Technologies
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    count,
    sum,
    avg,
    round,
    current_timestamp
)

spark = SparkSession.builder.appName(
    "BillingAnalytics"
).getOrCreate()


def load_billing_data():
    """
    Load consumer billing data.
    """
    return spark.table(
        "eagleeyelakebase_uc.energy.consumer_billing"
    )


def billing_summary(df):
    """
    Generate payment status summary.
    """
    return (
        df.groupBy("payment_status")
        .agg(
            count("*").alias("bill_count"),
            sum("total_amount").alias("total_collection"),
            round(
                avg("total_amount"),
                2
            ).alias("average_bill")
        )
    )


def save_summary(df):
    """
    Save billing summary.
    """
    (
        df.withColumn(
            "processed_timestamp",
            current_timestamp()
        )
        .write
        .mode("overwrite")
        .saveAsTable(
            "eagleeyelakebase_uc.energy.billing_summary"
        )
    )


def main():

    try:

        billing_df = load_billing_data()

        summary_df = billing_summary(
            billing_df
        )

        save_summary(summary_df)

        summary_df.show()

        print("Billing analytics completed successfully.")

    except Exception as error:

        print(f"Billing pipeline failed : {error}")


if __name__ == "__main__":
    main()
    