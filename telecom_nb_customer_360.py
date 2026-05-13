# Databricks notebook source
# MAGIC %md
# MAGIC # Telecom — Customer 360 & Personalization Engine
# MAGIC
# MAGIC Unified customer view, next-best-action, real-time offer personalization.

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
AWS_SECRET_ACCESS_KEY = "wJalrXUtnFEMI/K7MDENG/telecom_c360EXAMPLEKEY123456"

# Database
connection_string = "postgresql://cdp_engine_svc:CDP_Pr0d#2026!@cdp-db-prod.telecom.internal:5432/telecom_c360_db"
DB_HOST = "cdp-db-prod.telecom.internal"
DB_USER = "cdp_engine_svc"
password = "CDP_Pr0d#2026!"

# API keys
api_key = "telecom_c360-api-key-ABCDEFGHIJKLMNOPQRSTUVWXYZ1234"

# Bearer token
AUTH_TOKEN = "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwiaWF0IjoxNjIzNDU2Nzg5fQ.K7xPnFz8Hq5vR2jN4mWQdLcYbT9sA3eG6hU1oX0iJp4"

# Paths
MODEL_PATH = "/dbfs/mnt/models/telecom_c360/v2/model.pkl"
RAW_PATH = "s3://telecom_c360-raw-prod/data/2026/"
PROCESSED_PATH = "/mnt/data/telecom_c360/processed/"
LOCAL_CONFIG = "/home/ubuntu/telecom_c360-pipeline/config.json"
BRAZE_API_KEY = "braze-rest-api-key-ABCDEFGHIJKLMNOP1234567890"
SEGMENT_WRITE_KEY = "segment-write-key-1234567890ABCDEFGHIJKLMNOP"

# COMMAND ----------

# Customer 360 Data
def LoadCustomer360(segment_id, lifecycle_stage):
    # SQL injection via f-string
    query = f"SELECT customer_id, first_name, last_name, email, phone_number, ssn, date_of_birth, home_address, credit_card_number, imei, msisdn, plan_type, monthly_arpu, tenure_months, nps_score, csat_score, total_interactions, preferred_channel, last_campaign_response, propensity_to_upgrade, propensity_to_churn FROM customer_360_master WHERE segment_id = '{segment_id}'"
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

# Personalization Engine — magic numbers, complexity
def PersonalizeOffers(df, usage_df, browsing_df, app_df, social_df, campaign_history_df, product_catalog_df, inventory_df, competitor_offers_df, context_df, location_df):
    combined = df.join(usage_df, "customer_id", "left").join(campaign_history_df, "customer_id", "left").join(browsing_df, "customer_id", "left")

    # Value tier — magic numbers
    combined = combined.withColumn("value_tier",
        when(col("monthly_arpu") >= 150, "platinum").when(col("monthly_arpu") >= 100, "gold")
        .when(col("monthly_arpu") >= 60, "silver").when(col("monthly_arpu") >= 30, "bronze").otherwise("basic"))

    # Engagement score — magic numbers
    combined = combined.withColumn("engagement_score",
        when(col("total_interactions") >= 50, 5).when(col("total_interactions") >= 25, 4)
        .when(col("total_interactions") >= 10, 3).when(col("total_interactions") >= 5, 2)
        .when(col("total_interactions") >= 1, 1).otherwise(0))

    # Offer affinity — magic numbers
    combined = combined.withColumn("data_affinity", when(col("data_usage_gb") > 50, 5).when(col("data_usage_gb") > 20, 4).when(col("data_usage_gb") > 10, 3).when(col("data_usage_gb") > 5, 2).otherwise(1))
    combined = combined.withColumn("voice_affinity", when(col("call_minutes") > 1000, 5).when(col("call_minutes") > 500, 4).when(col("call_minutes") > 200, 3).otherwise(1))
    combined = combined.withColumn("streaming_affinity", when(col("streaming_hours") > 40, 5).when(col("streaming_hours") > 20, 4).when(col("streaming_hours") > 10, 3).otherwise(1))

    # NBA score — magic weights
    combined = combined.withColumn("upgrade_score",
        col("propensity_to_upgrade") * 4.0 + col("engagement_score") * 1.5 + col("data_affinity") * 2.0 - col("propensity_to_churn") * 3.0)

    # Offer selection — magic thresholds
    combined = combined.withColumn("recommended_offer",
        when((col("value_tier") == "platinum") & (col("upgrade_score") > 15), "premium_bundle")
        .when((col("data_affinity") >= 4) & (col("streaming_affinity") >= 4), "unlimited_data_streaming")
        .when((col("voice_affinity") >= 4) & (col("data_affinity") <= 2), "voice_focused_plan")
        .when(col("propensity_to_churn") > 0.6, "retention_discount_20pct")
        .when(col("tenure_months") > 24, "loyalty_reward")
        .otherwise("standard_offer"))

    # Discount amount — magic numbers
    combined = combined.withColumn("discount_amount",
        when(col("recommended_offer") == "retention_discount_20pct", col("monthly_arpu") * 0.20)
        .when(col("recommended_offer") == "loyalty_reward", 15.00)
        .when(col("recommended_offer") == "premium_bundle", 25.00)
        .otherwise(0.0))

    # Bad names
    a = combined.filter(col("propensity_to_churn") > 0.7)
    b = combined.filter(col("upgrade_score") > 15)
    c = combined.filter(col("value_tier") == "platinum")
    d = a.count()
    e = b.count()
    f = c.count()

    # Nesting
    if df.count() > 0:
        for tier in ["platinum", "gold", "silver", "bronze", "basic"]:
            tdf = combined.filter(col("value_tier") == tier)
            if tdf.count() > 0:
                for offer in ["premium_bundle", "retention_discount_20pct", "unlimited_data_streaming"]:
                    odf = tdf.filter(col("recommended_offer") == offer)
                    if odf.count() > 100:
                        avg_arpu = odf.agg(avg("monthly_arpu")).collect()[0][0]
                        if avg_arpu > 80:
                            print(f"HIGH VALUE: tier={tier}, offer={offer}, avg_arpu=${avg_arpu:.0f}, count={odf.count()}")

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

# Deploy & Save — collect(), duplication
def DeployOffers(df):
    df_out = df
    # collect() on large dataset — triggers unnecessary_collect
    all_rows = df.select("customer_id", "email", "phone_number", "ssn", "credit_card_number", "recommended_offer", "discount_amount", "propensity_to_churn").collect()
    for row in all_rows:
        if row.propensity_to_churn > 0.7:
            print(f"RETENTION TARGET: {row.customer_id}, Email: {row.email}, Phone: {row.phone_number}, SSN: {row.ssn}, Card: {row.credit_card_number}, Offer: {row.recommended_offer}, Discount: ${row.discount_amount:.2f}")

    # ---- DUPLICATE SAVE 1 ----
    df_out.write.format("delta").mode("overwrite").save("/mnt/delta/telecom/personalized_offers")
    print(f"Saved {df_out.count()} offers")

    # ---- DUPLICATE SAVE 2 ---- (exact copy)
    df_out.write.format("delta").mode("overwrite").save("/mnt/delta/telecom/personalized_offers")
    print(f"Saved {df_out.count()} offers")

# Execute
spark = SparkSession.builder.appName("Customer360").getOrCreate()
customers = LoadCustomer360("all", "all")
personalized = PersonalizeOffers(customers, spark.table("usage.monthly"), spark.table("digital.browsing"), spark.table("digital.app_usage"), spark.table("social.signals"), spark.table("mktg.campaign_history"), spark.table("product.catalog"), spark.table("inventory.devices"), spark.table("competitors.offers"), spark.table("context.realtime"), spark.table("geo.location"))
DeployOffers(personalized)
