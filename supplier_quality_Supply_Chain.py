"""
Supplier Quality Analysis

Project : Supply Chain Analytics Platform

Description:
Evaluates supplier performance based on
supplier ratings and purchase order value.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    sum,
    count,
    col,
    when,
    round,
)

spark = SparkSession.builder.appName(
    "Supplier Quality Analysis"
).getOrCreate()

CATALOG = "eagleeyelakebase_uc"
SCHEMA = "supply_chain_demo"

supplier_df = spark.table(
    f"{CATALOG}.{SCHEMA}.supplier_master"
)

purchase_df = spark.table(
    f"{CATALOG}.{SCHEMA}.purchase_orders"
)

supplier_summary = (
    purchase_df.join(
        supplier_df,
        "supplier_id",
        "inner"
    )
    .groupBy(
        "supplier_id",
        "supplier_name",
        "supplier_rating"
    )
    .agg(
        count("purchase_order_id").alias("total_orders"),
        sum("total_amount").alias("purchase_amount"),
        avg("total_amount").alias("average_order_value")
    )
)

supplier_summary = supplier_summary.withColumn(
    "supplier_grade",
    when(
        col("supplier_rating") >= 4.5,
        "Excellent"
    ).when(
        col("supplier_rating") >= 4.0,
        "Good"
    ).when(
        col("supplier_rating") >= 3.0,
        "Average"
    ).otherwise(
        "Needs Improvement"
    )
)

supplier_summary = supplier_summary.withColumn(
    "average_order_value",
    round(col("average_order_value"), 2)
)

print("Supplier Performance Summary")

supplier_summary.orderBy(
    col("purchase_amount").desc()
).show(20, False)

supplier_summary.createOrReplaceTempView(
    "supplier_performance"
)

spark.sql("""
SELECT
    supplier_grade,
    COUNT(*) AS suppliers,
    SUM(purchase_amount) AS total_purchase
FROM supplier_performance
GROUP BY supplier_grade
ORDER BY total_purchase DESC
""").show()

print("Supplier Quality Analysis Completed.")