"""
Project     : Energy Analytics Platform
Notebook    : Revenue Analytics
Description : Generate revenue KPIs and plant-wise revenue summary.
Author      : Celebal Technologies
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    sum,
    avg,
    round,
    current_timestamp
)

spark = SparkSession.builder.appName(
    "RevenueAnalytics"
).getOrCreate()


def load_revenue_data():
    """
    Load revenue fact table.
    """
    return spark.table(
        "eagleeyelakebase_uc.energy.revenue_fact"
    )


def revenue_summary(df):
    """
    Generate plant-wise revenue summary.
    """
    return (
        df.groupBy("plant_id")
        .agg(
            sum("revenue_amount").alias("total_revenue"),
            round(
                avg("revenue_amount"),
                2
            ).alias("average_revenue")
        )
    )


def save_summary(df):
    """
    Save revenue summary.
    """
    (
        df.withColumn(
            "processed_timestamp",
            current_timestamp()
        )
        .write
        .mode("overwrite")
        .saveAsTable(
            "eagleeyelakebase_uc.energy.revenue_summary"
        )
    )


def main():

    try:

        revenue_df = load_revenue_data()

        summary_df = revenue_summary(
            revenue_df
        )

        save_summary(summary_df)

        summary_df.show()

        print("Revenue analytics completed successfully.")

    except Exception as error:

        print(f"Revenue pipeline failed : {error}")


if __name__ == "__main__":
    main()
    