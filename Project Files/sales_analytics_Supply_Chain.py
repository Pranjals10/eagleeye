"""
Sales Analytics Pipeline

Project : Supply Chain Analytics Platform

Description:
Generates sales KPIs, customer revenue,
regional sales, and profitability analysis.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    sum,
    avg,
    count,
    col,
    round,
)

spark = SparkSession.builder.appName(
    "Sales Analytics"
).getOrCreate()

CATALOG = "eagleeyelakebase_uc"
SCHEMA = "supply_chain_demo"

sales_df = spark.table(
    f"{CATALOG}.{SCHEMA}.sales_fact"
)

customer_df = spark.table(
    f"{CATALOG}.{SCHEMA}.customer_orders"
)

sales_summary = (
    sales_df.groupBy("region")
    .agg(
        count("sales_id").alias("transactions"),
        sum("sales_amount").alias("total_sales"),
        sum("profit_amount").alias("total_profit"),
        avg("sales_amount").alias("average_sales")
    )
)

sales_summary = sales_summary.withColumn(
    "average_sales",
    round(col("average_sales"), 2)
)

print("Regional Sales Performance")

sales_summary.orderBy(
    col("total_sales").desc()
).show(20, False)

customer_sales = (
    sales_df.join(
        customer_df,
        "customer_order_id",
        "inner"
    )
    .groupBy("customer_id")
    .agg(
        sum("sales_amount").alias("customer_sales"),
        sum("profit_amount").alias("customer_profit")
    )
)

print("Top Customers")

customer_sales.orderBy(
    col("customer_sales").desc()
).show(10, False)

sales_df.createOrReplaceTempView(
    "sales_view"
)

spark.sql("""
SELECT
    YEAR(sales_date) AS sales_year,
    MONTH(sales_date) AS sales_month,
    SUM(sales_amount) AS total_sales,
    SUM(profit_amount) AS total_profit
FROM sales_view
GROUP BY
    YEAR(sales_date),
    MONTH(sales_date)
ORDER BY
    sales_year,
    sales_month
""").show()

print("Sales Analytics Completed Successfully.")