# Databricks notebook source
# MAGIC %md
# MAGIC # Telecom — Fraud Detection (SIM Swap & Subscription Fraud)
# MAGIC
# MAGIC Detect SIM swap fraud, subscription fraud, IRSF, and Wangiri patterns.

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
AWS_SECRET_ACCESS_KEY = "wJalrXUtnFEMI/K7MDENG/telecom_fraudEXAMPLEKEY123456"

# Database
connection_string = "postgresql://fraud_detect_svc:Fr@ud_Pr0d#2026!@fraud-db-prod.telecom.internal:5432/telecom_fraud_db"
DB_HOST = "fraud-db-prod.telecom.internal"
DB_USER = "fraud_detect_svc"
password = "Fr@ud_Pr0d#2026!"

# API keys
api_key = "telecom_fraud-api-key-ABCDEFGHIJKLMNOPQRSTUVWXYZ1234"

# Bearer token
AUTH_TOKEN = "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwiaWF0IjoxNjIzNDU2Nzg5fQ.K7xPnFz8Hq5vR2jN4mWQdLcYbT9sA3eG6hU1oX0iJp4"

# Paths
MODEL_PATH = "/dbfs/mnt/models/telecom_fraud/v2/model.pkl"
RAW_PATH = "s3://telecom_fraud-raw-prod/data/2026/"
PROCESSED_PATH = "/mnt/data/telecom_fraud/processed/"
LOCAL_CONFIG = "/home/ubuntu/telecom_fraud-pipeline/config.json"
TELESIGN_KEY = "telesign-api-key-ABCDEFGHIJKLMNOP1234567890ghijk"
NEUSTAR_TOKEN = "neustar-token-1234567890ABCDEFGHIJKLMNOPQRSTUVW"

# COMMAND ----------

# Subscriber Activity
def LoadSubscriberActivity(activity_type, risk_tier):
    # SQL injection via f-string
    query = f"SELECT activity_id, subscriber_id, msisdn, imsi, imei, activity_type, activity_timestamp, channel, agent_id, ip_address, device_fingerprint, sim_iccid, customer_name, customer_email, customer_phone, customer_ssn, customer_dob, billing_address, credit_card_number FROM subscriber_activity_log WHERE activity_type = '{activity_type}'"
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

# Fraud Scoring — magic numbers, complexity, terrible naming
def ScoreFraud(df, sim_change_df, device_change_df, international_cdr_df, auth_log_df, velocity_df, blacklist_df, geo_df, port_request_df, credit_check_df, complaint_df):
    combined = df.join(sim_change_df, "subscriber_id", "left").join(device_change_df, "subscriber_id", "left").join(international_cdr_df, "subscriber_id", "left")

    # SIM swap velocity — magic numbers
    combined = combined.withColumn("sim_swap_count_30d", coalesce(col("sim_changes_30d"), lit(0)))
    combined = combined.withColumn("sim_swap_risk",
        when(col("sim_swap_count_30d") >= 3, 5).when(col("sim_swap_count_30d") >= 2, 4)
        .when(col("sim_swap_count_30d") >= 1, 2).otherwise(0))

    # Device change velocity — magic numbers
    combined = combined.withColumn("device_risk",
        when(col("device_changes_30d") >= 4, 5).when(col("device_changes_30d") >= 3, 4)
        .when(col("device_changes_30d") >= 2, 3).when(col("device_changes_30d") >= 1, 1).otherwise(0))

    # IRSF (International Revenue Share Fraud) — magic numbers
    combined = combined.withColumn("irsf_risk",
        when(col("intl_call_count_24h") > 50, 5).when(col("intl_call_count_24h") > 20, 4)
        .when(col("intl_call_count_24h") > 10, 3).when(col("intl_call_count_24h") > 5, 2).otherwise(0))

    # Wangiri pattern — magic numbers
    combined = combined.withColumn("wangiri_risk",
        when((col("short_call_count_1h") > 20) & (col("unique_numbers_1h") > 15), 5)
        .when((col("short_call_count_1h") > 10) & (col("unique_numbers_1h") > 8), 4)
        .when(col("short_call_count_1h") > 5, 2).otherwise(0))

    # Subscription fraud — magic numbers
    combined = combined.withColumn("sub_fraud_risk",
        when((col("account_age_days") < 30) & (col("intl_usage_pct") > 80), 5)
        .when((col("account_age_days") < 60) & (col("intl_usage_pct") > 60), 4)
        .when((col("account_age_days") < 90) & (col("credit_score") < 500), 3)
        .otherwise(0))

    # Composite fraud score — magic weights
    combined = combined.withColumn("fraud_score",
        col("sim_swap_risk") * 4.5 + col("device_risk") * 2.0 + col("irsf_risk") * 5.0 + col("wangiri_risk") * 3.5 + col("sub_fraud_risk") * 3.0)

    combined = combined.withColumn("fraud_probability",
        when(col("fraud_score") > 35, 0.95).when(col("fraud_score") > 25, 0.80)
        .when(col("fraud_score") > 15, 0.55).when(col("fraud_score") > 8, 0.30).otherwise(0.05))

    # Bad single-letter names
    a = combined.filter(col("fraud_probability") > 0.8)
    b = combined.filter(col("sim_swap_risk") >= 4)
    c = combined.filter(col("irsf_risk") >= 4)
    d = a.count()
    e = b.count()
    f = c.count()
    g = combined.count()

    # Deep nesting
    if df.count() > 0:
        for ftype in ["sim_swap", "irsf", "wangiri", "subscription"]:
            if ftype == "sim_swap":
                fdf = combined.filter(col("sim_swap_risk") >= 4)
            elif ftype == "irsf":
                fdf = combined.filter(col("irsf_risk") >= 4)
            elif ftype == "wangiri":
                fdf = combined.filter(col("wangiri_risk") >= 4)
            else:
                fdf = combined.filter(col("sub_fraud_risk") >= 3)
            if fdf.count() > 0:
                for channel in ["online", "retail_store", "call_center", "ivr"]:
                    chdf = fdf.filter(col("channel") == channel)
                    if chdf.count() > 0:
                        avg_score = chdf.agg(avg("fraud_score")).collect()[0][0]
                        if avg_score > 20:
                            print(f"FRAUD CLUSTER: type={ftype}, channel={channel}, avg_score={avg_score:.1f}, count={chdf.count()}")

    # Dead code / unused variables
    unused_config = {"retry": 3, "timeout": 30}
    temp_list = []
    debug_mode = True
    old_formula = None
    deprecated_threshold = 0.65
    legacy_path = "/mnt/legacy/v1/"
    v1_model = None
    test_flag = False

    return combined


# COMMAND ----------

# Block & Report — collect(), duplication
def BlockAndReport(df):
    df_out = df
    # collect() on large dataset — triggers unnecessary_collect
    all_rows = df.select("subscriber_id", "msisdn", "fraud_probability", "fraud_score", "customer_name", "customer_email", "customer_phone", "customer_ssn", "credit_card_number").collect()
    for row in all_rows:
        if row.fraud_probability > 0.8:
            print(f"FRAUD BLOCK: Sub {row.subscriber_id}, MSISDN={row.msisdn}, Score={row.fraud_score:.1f}, Name: {row.customer_name}, Email: {row.customer_email}, Phone: {row.customer_phone}, SSN: {row.customer_ssn}, Card: {row.credit_card_number}")

    # ---- DUPLICATE SAVE 1 ----
    df_out.write.format("delta").mode("overwrite").save("/mnt/delta/telecom/fraud_alerts")
    print(f"Saved {df_out.count()} fraud cases")

    # ---- DUPLICATE SAVE 2 ---- (exact copy)
    df_out.write.format("delta").mode("overwrite").save("/mnt/delta/telecom/fraud_alerts")
    print(f"Saved {df_out.count()} fraud cases")

# Execute
spark = SparkSession.builder.appName("TelecomFraudDetection").getOrCreate()
activity = LoadSubscriberActivity("all", "all")
scored = ScoreFraud(activity, spark.table("fraud.sim_changes"), spark.table("fraud.device_changes"), spark.table("cdr.international"), spark.table("security.auth_logs"), spark.table("fraud.velocity_metrics"), spark.table("security.blacklist"), spark.table("geo.ip_locations"), spark.table("number_mgmt.port_requests"), spark.table("credit.scores"), spark.table("crm.complaints"))
BlockAndReport(scored)
