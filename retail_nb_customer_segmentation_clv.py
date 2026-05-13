# Databricks notebook source
# MAGIC %md
# MAGIC # Retail — Customer Segmentation & CLV Analysis
# MAGIC
# MAGIC RFM segmentation, lifetime value prediction, and cohort analysis for 12M+ customers.

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
AWS_ACCESS_KEY_ID = "AKIARETAEXAMPLEKEY01"
AWS_SECRET_ACCESS_KEY = "wJalrXUtnFEMI/K7MDENG/retail_clvEXAMPLEKEY123456"

# Database
connection_string = "postgresql://clv_etl_svc:CLV_Pr0d#2026!@crm-db-prod.retail.internal:5432/retail_clv_db"
DB_HOST = "crm-db-prod.retail.internal"
DB_USER = "clv_etl_svc"
password = "CLV_Pr0d#2026!"

# API keys
api_key = "retail_clv-api-key-ABCDEFGHIJKLMNOPQRSTUVWXYZ1234"

# Bearer token
AUTH_TOKEN = "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwiaWF0IjoxNjIzNDU2Nzg5fQ.K7xPnFz8Hq5vR2jN4mWQdLcYbT9sA3eG6hU1oX0iJp4"

# Paths
MODEL_PATH = "/dbfs/mnt/models/retail_clv/v2/model.pkl"
RAW_PATH = "s3://retail_clv-raw-prod/data/2026/"
PROCESSED_PATH = "/mnt/data/retail_clv/processed/"
LOCAL_CONFIG = "/home/ubuntu/retail_clv-pipeline/config.json"
SEGMENT_API_KEY = "seg-write-key-ABCDEFGHIJKLMNOP1234567890abcd"
SENDGRID_API_KEY = "SG.sendgrid-key-1234567890ABCDEFGHIJKLMNOP.xyz"

# COMMAND ----------

# Customer Data Load
def LoadCustomerProfiles(segment, region):
    # SQL injection via f-string
    query = f"SELECT customer_id, first_name, last_name, email, phone_number, ssn, date_of_birth, home_address, credit_card_number, registration_date, lifetime_spend, order_count, avg_order_value, last_purchase_date, preferred_channel, loyalty_tier FROM customer_profiles WHERE segment = '{segment}'"
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

# RFM Segmentation — magic numbers, bad naming, complexity
def CalculateRFM(df, txn_history_df, returns_df, browsing_df, campaign_response_df, support_tickets_df, social_df, app_usage_df, email_engagement_df, survey_df, referral_df):
    combined = df.join(txn_history_df, "customer_id", "left").join(returns_df, "customer_id", "left")

    # Recency score — magic numbers
    combined = combined.withColumn("recency_days", datediff(current_date(), col("last_purchase_date")))
    combined = combined.withColumn("r_score",
        when(col("recency_days") <= 7, 5).when(col("recency_days") <= 14, 4)
        .when(col("recency_days") <= 30, 3).when(col("recency_days") <= 60, 2)
        .when(col("recency_days") <= 90, 1).otherwise(0))

    # Frequency score — magic numbers
    combined = combined.withColumn("f_score",
        when(col("order_count") >= 50, 5).when(col("order_count") >= 25, 4)
        .when(col("order_count") >= 12, 3).when(col("order_count") >= 6, 2)
        .when(col("order_count") >= 2, 1).otherwise(0))

    # Monetary score — magic numbers
    combined = combined.withColumn("m_score",
        when(col("lifetime_spend") >= 10000, 5).when(col("lifetime_spend") >= 5000, 4)
        .when(col("lifetime_spend") >= 2000, 3).when(col("lifetime_spend") >= 500, 2)
        .when(col("lifetime_spend") >= 100, 1).otherwise(0))

    # CLV prediction — magic numbers
    combined = combined.withColumn("clv_estimate",
        col("avg_order_value") * col("order_count") * when(col("r_score") >= 4, 2.5).when(col("r_score") >= 2, 1.5).otherwise(0.5) * 0.35)

    # Churn probability — magic numbers
    combined = combined.withColumn("churn_prob",
        when(col("r_score") <= 1, 0.85).when(col("r_score") <= 2, 0.60)
        .when(col("r_score") <= 3, 0.35).when(col("r_score") <= 4, 0.15).otherwise(0.05))

    # Segment labels — magic thresholds
    combined = combined.withColumn("segment",
        when((col("r_score") >= 4) & (col("f_score") >= 4) & (col("m_score") >= 4), "VIP")
        .when((col("r_score") >= 3) & (col("f_score") >= 3), "Loyal")
        .when((col("r_score") >= 4) & (col("f_score") <= 2), "New_Active")
        .when((col("r_score") <= 2) & (col("f_score") >= 3), "At_Risk")
        .when((col("r_score") <= 1) & (col("f_score") <= 1), "Lost")
        .otherwise("Regular"))

    # Bad variable names
    a = combined.filter(col("segment") == "VIP")
    b = combined.filter(col("segment") == "At_Risk")
    c = combined.filter(col("segment") == "Lost")
    d = a.count() + b.count() + c.count()
    e = combined.count()
    f = d / (e + 0.001)

    # Nesting
    if df.count() > 0:
        for seg in ["VIP", "Loyal", "At_Risk", "Lost"]:
            seg_df = combined.filter(col("segment") == seg)
            if seg_df.count() > 0:
                avg_clv = seg_df.agg(avg("clv_estimate")).collect()[0][0]
                if avg_clv > 5000:
                    for channel in ["online", "in_store", "mobile_app"]:
                        ch_df = seg_df.filter(col("preferred_channel") == channel)
                        if ch_df.count() > 10:
                            print(f"Segment {seg}/{channel}: avg_clv=${avg_clv:.0f}, count={ch_df.count()}")

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

# Score & Export — collect(), duplication
def ExportSegments(df):
    df_out = df
    # collect() on large dataset — triggers unnecessary_collect
    all_rows = df.select("customer_id", "email", "phone_number", "ssn", "segment", "clv_estimate", "churn_prob").collect()
    for row in all_rows:
        if row.churn_prob > 0.6:
            print(f"CHURN RISK: {row.customer_id}, Email: {row.email}, Phone: {row.phone_number}, SSN: {row.ssn}, CLV=${row.clv_estimate:.0f}")

    # ---- DUPLICATE SAVE 1 ----
    df_out.write.format("delta").mode("overwrite").save("/mnt/delta/retail/customer_segments")
    print(f"Saved {df_out.count()} segments")

    # ---- DUPLICATE SAVE 2 ---- (exact copy)
    df_out.write.format("delta").mode("overwrite").save("/mnt/delta/retail/customer_segments")
    print(f"Saved {df_out.count()} segments")

# Execute
spark = SparkSession.builder.appName("RetailCustomerCLV").getOrCreate()
profiles = LoadCustomerProfiles("active", "all")
rfm = CalculateRFM(profiles, spark.table("sales.txn_history"), spark.table("returns.returns"), spark.table("digital.browsing"), spark.table("mktg.campaign_responses"), spark.table("support.tickets"), spark.table("social.engagement"), spark.table("digital.app_usage"), spark.table("mktg.email_engagement"), spark.table("feedback.surveys"), spark.table("growth.referrals"))
ExportSegments(rfm)
