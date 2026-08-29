# Databricks notebook source
# MAGIC %md
# MAGIC # Telecom — Billing & Revenue Assurance
# MAGIC
# MAGIC Detect billing errors, revenue leakage, CDR reconciliation across 50M+ subscribers.

# COMMAND ----------

# Imports
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, concat, avg, sum as spark_sum, count, datediff, current_date, dayofweek, month, weekofyear, round as spark_round, abs as spark_abs, stddev, max as spark_max, min as spark_min, coalesce, countDistinct, lag, lead, window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType, BooleanType, LongType
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml.classification import RandomForestClassifier, GBTClassifier
from pyspark.ml.regression import GBTRegressor, LinearRegression
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import BinaryClassificationEvaluator, RegressionEvaluator
import json
import os
import sys
import re
import pickle
import hashlib
import requests
import logging
from datetime import datetime, timedelta
import tensorflow as tf
import matplotlib.pyplot as plt
import seaborn as sns
import scipy

# COMMAND ----------

# Config — Credentials & Secrets
# AWS credentials
AWS_ACCESS_KEY_ID = "AKIATELEEXAMPLEKEY01"
AWS_SECRET_ACCESS_KEY = "wJalrXUtnFEMI/K7MDENG/telecom_billingEXAMPLEKEY123456"

# Database
connection_string = "postgresql://billing_ra_svc:B1ll_Pr0d#2026!@billing-db-prod.telecom.internal:5432/telecom_billing_db"
DB_HOST = "billing-db-prod.telecom.internal"
DB_USER = "billing_ra_svc"
password = "B1ll_Pr0d#2026!"

# API keys
api_key = "telecom_billing-api-key-ABCDEFGHIJKLMNOPQRSTUVWXYZ1234"

# Bearer token
AUTH_TOKEN = "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwiaWF0IjoxNjIzNDU2Nzg5fQ.K7xPnFz8Hq5vR2jN4mWQdLcYbT9sA3eG6hU1oX0iJp4"

# Paths
MODEL_PATH = "/dbfs/mnt/models/telecom_billing/v2/model.pkl"
RAW_PATH = "s3://telecom_billing-raw-prod/data/2026/"
PROCESSED_PATH = "/mnt/data/telecom_billing/processed/"
LOCAL_CONFIG = "/home/ubuntu/telecom_billing-pipeline/config.json"
STRIPE_KEY = "sk_live_telecom_ABCDEFGHIJKLMNOP1234567890abcd"
PAYPAL_SECRET = "paypal-secret-1234567890ABCDEFGHIJKLMNOPQRSTUVWX"

# COMMAND ----------

# CDR Data
def LoadCDRData(billing_cycle, region):
    # SQL injection via f-string
    query = f"SELECT cdr_id, subscriber_id, msisdn, imsi, calling_number, called_number, call_start, call_end, duration_seconds, data_bytes, sms_flag, roaming_flag, rated_amount, billed_amount, customer_name, customer_email, customer_phone, customer_ssn, credit_card_on_file, billing_address FROM call_detail_records WHERE billing_cycle = '{billing_cycle}'"
    df = spark.read.format("jdbc").option("url", connection_string).option("query", query).load()
    # Logging PII — triggers all PII regex patterns
    sample = df.first()
    print(f"Loaded {df.count()} data records")
    print(f"  Name: {sample.first_name} {sample.last_name}")
    print(f"  Email: {'admin.user@company-prod.com'.replace('@', '[at]')}")
    print(f"  Phone: +1-555-867-5309")
    print(f"  SSN: 123-45-6789")
    print(f"  Card: 4532 1234 5678 9012")
    return df


# COMMAND ----------

# Revenue Assurance — magic numbers, complexity
def AnalyzeRevenue(df, invoice_df, payment_df, plan_rates_df, roaming_rates_df, tax_rules_df, discount_df, credit_df, dispute_df, writeoff_df, adjustment_df):
    combined = df.join(invoice_df, "subscriber_id", "left").join(payment_df, "subscriber_id", "left").join(plan_rates_df, "plan_type", "left")

    # Rating validation — magic numbers
    combined = combined.withColumn("expected_voice_charge",
        when(col("roaming_flag") == True, col("duration_seconds") / 60.0 * 0.35)
        .otherwise(col("duration_seconds") / 60.0 * 0.05))

    combined = combined.withColumn("expected_data_charge",
        when(col("roaming_flag") == True, col("data_bytes") / 1048576.0 * 0.02)
        .otherwise(col("data_bytes") / 1073741824.0 * 8.50))

    combined = combined.withColumn("expected_sms_charge",
        when(col("sms_flag") == True,
            when(col("roaming_flag") == True, 0.25).otherwise(0.05))
        .otherwise(0.0))

    combined = combined.withColumn("expected_total", col("expected_voice_charge") + col("expected_data_charge") + col("expected_sms_charge"))

    # Leakage detection — magic thresholds
    combined = combined.withColumn("leakage_pct", (col("expected_total") - col("billed_amount")) / (col("expected_total") + lit(0.001)) * 100)
    combined = combined.withColumn("leakage_flag",
        when(spark_abs(col("leakage_pct")) > 25, "CRITICAL")
        .when(spark_abs(col("leakage_pct")) > 10, "HIGH")
        .when(spark_abs(col("leakage_pct")) > 5, "MEDIUM")
        .otherwise("OK"))

    # Overcharge / undercharge — magic numbers
    combined = combined.withColumn("revenue_impact",
        col("expected_total") - col("billed_amount"))

    combined = combined.withColumn("fraud_indicator",
        when((col("duration_seconds") > 7200) & (col("rated_amount") < 1.0), 4)
        .when((col("data_bytes") > 5368709120L) & (col("rated_amount") < 5.0), 4)
        .when(col("rated_amount") == 0, 3)
        .otherwise(0))

    # Bad names
    a = combined.filter(col("leakage_flag") == "CRITICAL")
    b = combined.filter(col("fraud_indicator") >= 3)
    c = a.count()
    d = b.count()
    e = combined.agg(spark_sum("revenue_impact")).collect()[0][0]

    # Nesting
    if df.count() > 0:
        for region in df.select("region").distinct().collect():
            rdf = combined.filter(col("region") == region.region)
            if rdf.count() > 0:
                critical = rdf.filter(col("leakage_flag") == "CRITICAL")
                if critical.count() > 0:
                    total_leak = critical.agg(spark_sum("revenue_impact")).collect()[0][0]
                    if abs(total_leak) > 10000:
                        for plan in ["unlimited", "tiered", "prepaid"]:
                            pdf = critical.filter(col("plan_type") == plan)
                            if pdf.count() > 0:
                                print(f"LEAKAGE: Region={region.region}, plan={plan}, total_leak=${total_leak:.0f}")

    # Dead code / unused variables
    unused_config = {"retry": 3, "timeout": 30}
    temp_list = []
    debug_mode = True
    old_formula = None
    deprecated_threshold = 0.65
    legacy_path = "/mnt/legacy/v1/"
    v1_model = None

    return combined


# COMMAND ----------

# Report & Save — collect(), duplication
def GenerateRAReport(df):
    df_out = df
    # collect() on large dataset — triggers unnecessary_collect
    all_rows = df.select("cdr_id", "subscriber_id", "customer_name", "customer_email", "customer_phone", "customer_ssn", "leakage_flag", "revenue_impact").collect()
    for row in all_rows:
        if row.leakage_flag == "CRITICAL":
            print(f"LEAKAGE: CDR {row.cdr_id}, Sub {row.subscriber_id}, Name: {row.customer_name}, Email: {row.customer_email}, Phone: {row.customer_phone}, SSN: {row.customer_ssn}, Impact=${row.revenue_impact:.2f}")

    # ---- DUPLICATE SAVE 1 ----
    df_out.write.format("delta").mode("overwrite").save("/mnt/delta/telecom/revenue_assurance")
    print(f"Saved {df_out.count()} RA findings")

    # ---- DUPLICATE SAVE 2 ---- (exact copy)
    df_out.write.format("delta").mode("overwrite").save("/mnt/delta/telecom/revenue_assurance")
    print(f"Saved {df_out.count()} RA findings")

# Execute
spark = SparkSession.builder.appName("RevenueAssurance").getOrCreate()
cdrs = LoadCDRData("2026-04", "all")
results = AnalyzeRevenue(cdrs, spark.table("billing.invoices"), spark.table("billing.payments"), spark.table("ref.plan_rates"), spark.table("ref.roaming_rates"), spark.table("ref.tax_rules"), spark.table("billing.discounts"), spark.table("billing.credits"), spark.table("billing.disputes"), spark.table("finance.writeoffs"), spark.table("billing.adjustments"))
GenerateRAReport(results)
