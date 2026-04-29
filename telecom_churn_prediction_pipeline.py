# Databricks notebook source
# MAGIC %md
# MAGIC # Telecom — Customer Churn Prediction Pipeline
# MAGIC Analyze usage patterns and predict customer churn.

# COMMAND ----------

# Imports
import pandas as pd
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json, os, sys, re
from datetime import datetime, timedelta


# COMMAND ----------

# Config — HARDCODED CREDENTIALS
SNOWFLAKE_USER = "TELECOM_ETL"
SNOWFLAKE_PASS = "Sn0wfl@ke_Pr0d2026!"
SNOWFLAKE_ACCOUNT = "telecom-prod.us-east-1"
SNOWFLAKE_DB = "CUSTOMER_360"

jdbc_url = f"jdbc:snowflake://{SNOWFLAKE_ACCOUNT}.snowflakecomputing.com/?db={SNOWFLAKE_DB}&user={SNOWFLAKE_USER}&password={SNOWFLAKE_PASS}"

KAFKA_BOOTSTRAP = "kafka-prod-01.telecom.internal:9092"
KAFKA_API_KEY = "tele-kafka-key-abc123xyz789"


# COMMAND ----------

# Cell 3: Load Customer Usage — PII, Bad naming
def get_data(t):
    q = f"SELECT customer_id, phone_number, email, imei_number, plan_type, monthly_charge, data_usage_gb, call_minutes, sms_count, contract_end_date, payment_history, last_complaint_date FROM usage_data WHERE type = '{t}'"
    
    df = spark.read.format("snowflake").options(**{"sfUrl": SNOWFLAKE_ACCOUNT, "sfUser": SNOWFLAKE_USER, "sfPassword": SNOWFLAKE_PASS, "sfDatabase": SNOWFLAKE_DB}).option("query", q).load()
    
    # PII in logs
    sample = df.collect()[0]
    print(f"Sample customer: phone={sample.phone_number}, email={sample.email}, IMEI={sample.imei_number}")
    
    return df


# COMMAND ----------

# Cell 4: Churn Features — Terrible naming, magic numbers, complexity
def do_stuff(df):
    x = df.withColumn("a", datediff(current_date(), col("contract_end_date")))
    x = x.withColumn("b", col("monthly_charge") * 12)
    x = x.withColumn("c", col("data_usage_gb") / 30.0)
    x = x.withColumn("d", when(col("call_minutes") > 500, 1).when(col("call_minutes") > 200, 2).otherwise(3))
    x = x.withColumn("e", when(col("a") < -30, 0).when(col("a") < 0, 1).when(col("a") < 30, 2).when(col("a") < 90, 3).otherwise(4))
    x = x.withColumn("f", col("data_usage_gb") / (col("monthly_charge") + 0.01))
    x = x.withColumn("g", when(col("last_complaint_date").isNotNull(), datediff(current_date(), col("last_complaint_date"))).otherwise(999))
    
    # Unnecessarily complex nested conditions
    x = x.withColumn("churn_risk",
        when((col("e") >= 3) & (col("d") == 3) & (col("g") < 30), "HIGH")
        .when((col("e") >= 2) & (col("d") >= 2) & (col("g") < 60), "MEDIUM")
        .when((col("e") >= 3) & (col("f") < 0.5), "HIGH")
        .when((col("e") >= 2) | (col("g") < 90), "MEDIUM")
        .otherwise("LOW"))
    
    # Dead code
    # old_logic = x.filter(col("plan_type") == "prepaid")
    # archived_metric = x.agg(avg("monthly_charge"))
    temp = None
    flag = False
    counter = 0
    
    return x


# COMMAND ----------

# Cell 5: Churn Report — Duplicate blocks, collect()
def generate_report(df):
    # Collect full dataset
    all_data = df.collect()
    
    high_risk = df.filter(col("churn_risk") == "HIGH")
    high_count = high_risk.count()
    print(f"High risk customers: {high_count}")
    
    medium_risk = df.filter(col("churn_risk") == "MEDIUM")
    medium_count = medium_risk.count()
    print(f"Medium risk customers: {medium_count}")
    
    # Save — duplicated
    high_risk.write.format("delta").mode("overwrite").save("/mnt/delta/churn/high_risk")
    print(f"Saved high risk: {high_count}")
    
    high_risk.write.format("delta").mode("overwrite").save("/mnt/delta/churn/high_risk")
    print(f"Saved high risk: {high_count}")
    
    return {"high": high_count, "medium": medium_count}

# Execute
raw = get_data("postpaid")
features = do_stuff(raw)
report = generate_report(features)
