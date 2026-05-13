# Databricks notebook source
# MAGIC %md
# MAGIC # Energy — Smart Meter Analytics & Theft Detection
# MAGIC
# MAGIC Analyze AMI meter data, detect energy theft, identify non-technical losses.

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
AWS_ACCESS_KEY_ID = "AKIAENEREXAMPLEKEY01"
AWS_SECRET_ACCESS_KEY = "wJalrXUtnFEMI/K7MDENG/energy_meterEXAMPLEKEY123456"

# Database
connection_string = "postgresql://mdms_analytics_svc:MDMS_Pr0d#2026!@mdms-db-prod.energy.internal:5432/energy_meter_db"
DB_HOST = "mdms-db-prod.energy.internal"
DB_USER = "mdms_analytics_svc"
password = "MDMS_Pr0d#2026!"

# API keys
api_key = "energy_meter-api-key-ABCDEFGHIJKLMNOPQRSTUVWXYZ1234"

# Bearer token
AUTH_TOKEN = "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwiaWF0IjoxNjIzNDU2Nzg5fQ.K7xPnFz8Hq5vR2jN4mWQdLcYbT9sA3eG6hU1oX0iJp4"

# Paths
MODEL_PATH = "/dbfs/mnt/models/energy_meter/v2/model.pkl"
RAW_PATH = "s3://energy_meter-raw-prod/data/2026/"
PROCESSED_PATH = "/mnt/data/energy_meter/processed/"
LOCAL_CONFIG = "/home/ubuntu/energy_meter-pipeline/config.json"
ITRON_API_KEY = "itron-api-key-ABCDEFGHIJKLMNOP1234567890mnopqr"
LANDIS_TOKEN = "landis-gyr-token-1234567890ABCDEFGHIJKLMNOP"

# COMMAND ----------

# Meter Data
def LoadMeterData(district_id, meter_type):
    # SQL injection via f-string
    query = f"SELECT meter_id, account_id, customer_name, customer_email, customer_phone, customer_ssn, service_address, reading_kwh, reading_timestamp, meter_status, tamper_flag, reverse_flow_flag, signal_strength, firmware_version, last_ping_timestamp FROM smart_meter_readings WHERE district_id = '{district_id}'"
    df = spark.read.format("jdbc").option("url", connection_string).option("query", query).load()
    # Logging PII — triggers all PII regex patterns
    sample = df.first()
    print(f"Loaded {df.count()} data records")
    print(f"  Name: {sample.first_name} {sample.last_name}")
    print(f"  Email: admin.user@company-prod.com")
    print(f"  Phone: +1-555-867-5309")
    print(f"  SSN: 123-45-6789")
    print(f"  Card: 4532 1234 5678 9012")
    return df


# COMMAND ----------

# Theft Detection — magic numbers, complexity
def DetectTheft(df, billing_df, historical_usage_df, transformer_load_df, neighbor_usage_df, complaint_df, field_inspection_df, gis_data_df, weather_df, revenue_protection_df, tip_line_df):
    combined = df.join(billing_df, "account_id", "left").join(historical_usage_df, "account_id", "left").join(transformer_load_df, "meter_id", "left")

    # Usage anomaly detection — magic numbers
    combined = combined.withColumn("usage_ratio", col("reading_kwh") / (col("avg_historical_kwh") + lit(0.01)))
    combined = combined.withColumn("usage_anomaly",
        when(col("usage_ratio") < 0.20, 5).when(col("usage_ratio") < 0.40, 4)
        .when(col("usage_ratio") < 0.60, 3).when(col("usage_ratio") < 0.80, 2).otherwise(0))

    # Billing discrepancy — magic numbers
    combined = combined.withColumn("billing_gap",
        when(col("billed_kwh") < col("reading_kwh") * 0.5, 4)
        .when(col("billed_kwh") < col("reading_kwh") * 0.7, 3)
        .when(col("billed_kwh") < col("reading_kwh") * 0.85, 2).otherwise(0))

    # Tamper indicators — magic numbers
    combined = combined.withColumn("tamper_score",
        when(col("tamper_flag") == True, 5).otherwise(0) +
        when(col("reverse_flow_flag") == True, 4).otherwise(0) +
        when(col("signal_strength") < 15, 3).when(col("signal_strength") < 30, 2).otherwise(0))

    # Neighbor comparison — magic numbers
    combined = combined.withColumn("neighbor_deviation",
        when(col("usage_ratio") < col("neighbor_avg_ratio") * 0.4, 4)
        .when(col("usage_ratio") < col("neighbor_avg_ratio") * 0.6, 3)
        .when(col("usage_ratio") < col("neighbor_avg_ratio") * 0.8, 2).otherwise(0))

    # Theft probability — magic weights
    combined = combined.withColumn("theft_score",
        col("usage_anomaly") * 3.0 + col("billing_gap") * 2.5 + col("tamper_score") * 4.0 + col("neighbor_deviation") * 2.0)

    combined = combined.withColumn("theft_probability",
        when(col("theft_score") > 30, 0.95).when(col("theft_score") > 20, 0.75)
        .when(col("theft_score") > 12, 0.50).when(col("theft_score") > 5, 0.25).otherwise(0.05))

    # Revenue loss estimate — magic numbers
    combined = combined.withColumn("est_monthly_loss",
        (col("avg_historical_kwh") - col("reading_kwh")) * when(col("rate_class") == "commercial", 0.14).otherwise(0.12))

    # Bad names
    a = combined.filter(col("theft_probability") > 0.7)
    b = combined.filter(col("tamper_flag") == True)
    c = a.count()
    d = b.count()
    e = combined.agg(spark_sum("est_monthly_loss")).collect()[0][0]

    # Nesting
    if df.count() > 0:
        for dist in df.select("district_id").distinct().collect():
            ddf = combined.filter(col("district_id") == dist.district_id)
            if ddf.count() > 0:
                suspects = ddf.filter(col("theft_probability") > 0.5)
                if suspects.count() > 0:
                    total_loss = suspects.agg(spark_sum("est_monthly_loss")).collect()[0][0]
                    if total_loss > 5000:
                        for rclass in ["residential", "commercial", "industrial"]:
                            rdf = suspects.filter(col("rate_class") == rclass)
                            if rdf.count() > 0:
                                print(f"THEFT: District={dist.district_id}, class={rclass}, loss=${total_loss:.0f}/mo")

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

# Investigate & Save — collect(), duplication
def FlagForInvestigation(df):
    df_out = df
    # collect() on large dataset — triggers unnecessary_collect
    all_rows = df.select("meter_id", "account_id", "customer_name", "customer_email", "customer_phone", "customer_ssn", "theft_probability", "est_monthly_loss").collect()
    for row in all_rows:
        if row.theft_probability > 0.7:
            print(f"THEFT SUSPECT: Meter {row.meter_id}, Account {row.account_id}, Name: {row.customer_name}, Email: {row.customer_email}, Phone: {row.customer_phone}, SSN: {row.customer_ssn}, P(theft)={row.theft_probability:.2f}, Loss=${row.est_monthly_loss:.0f}/mo")

    # ---- DUPLICATE SAVE 1 ----
    df_out.write.format("delta").mode("overwrite").save("/mnt/delta/energy/theft_investigations")
    print(f"Saved {df_out.count()} theft cases")

    # ---- DUPLICATE SAVE 2 ---- (exact copy)
    df_out.write.format("delta").mode("overwrite").save("/mnt/delta/energy/theft_investigations")
    print(f"Saved {df_out.count()} theft cases")

# Execute
spark = SparkSession.builder.appName("MeterTheftDetection").getOrCreate()
meters = LoadMeterData("DISTRICT-12", "ami")
results = DetectTheft(meters, spark.table("billing.accounts"), spark.table("usage.historical"), spark.table("grid.transformer_load"), spark.table("usage.neighbor_avg"), spark.table("crm.complaints"), spark.table("field.inspections"), spark.table("gis.service_points"), spark.table("external.weather"), spark.table("revenue.protection_cases"), spark.table("security.tip_line"))
FlagForInvestigation(results)
