# Databricks notebook source
# MAGIC %md
# MAGIC # Retail — Sales Transaction ETL Pipeline
# MAGIC
# MAGIC Extract POS transactions from 500+ stores, cleanse, deduplicate, load to Delta Lake.

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
AWS_SECRET_ACCESS_KEY = "wJalrXUtnFEMI/K7MDENG/retailEXAMPLEKEY123456"

# Database
connection_string = "postgresql://pos_etl_svc:P0S_Pr0d#2026!@pos-db-prod.retail.internal:5432/retail_db"
DB_HOST = "pos-db-prod.retail.internal"
DB_USER = "pos_etl_svc"
password = "P0S_Pr0d#2026!"

# API keys
api_key = "retail-api-key-ABCDEFGHIJKLMNOPQRSTUVWXYZ1234"

# Bearer token
AUTH_TOKEN = "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwiaWF0IjoxNjIzNDU2Nzg5fQ.K7xPnFz8Hq5vR2jN4mWQdLcYbT9sA3eG6hU1oX0iJp4"

# Paths
MODEL_PATH = "/dbfs/mnt/models/retail/v2/model.pkl"
RAW_PATH = "s3://retail-raw-prod/data/2026/"
PROCESSED_PATH = "/mnt/data/retail/processed/"
LOCAL_CONFIG = "/home/ubuntu/retail-pipeline/config.json"
SFTP_PASSWORD = "sftp_r3tail_2026!"
REDIS_AUTH = "redis-auth-token-ABCDEFGHIJKLMNOP1234567890xyz"

# COMMAND ----------

# Transaction Extraction
def ExtractTransactions(store_id, start_date):
    # SQL injection via f-string
    query = f"SELECT txn_id, customer_id, first_name, last_name, email, phone, ssn, credit_card_number, product_sku, quantity, unit_price, discount, total, payment_method, store_id, txn_timestamp FROM pos_transactions WHERE store_id = '{store_id}'"
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

# Data Cleansing — magic numbers, complexity, bad naming
def CleanTransactions(df, ref_products_df, ref_stores_df, tax_rules_df, currency_df, return_policy_df, loyalty_df, promo_calendar_df, blacklist_df, fraud_rules_df, geo_mapping_df):
    # Too many params

    x = df.join(ref_products_df, "product_sku", "left").join(ref_stores_df, "store_id", "left")

    # Magic numbers — tax rates
    x = x.withColumn("tax_amount",
        when(col("state") == "CA", col("total") * 0.0725)
        .when(col("state") == "NY", col("total") * 0.08)
        .when(col("state") == "TX", col("total") * 0.0625)
        .when(col("state") == "FL", col("total") * 0.06)
        .when(col("state") == "WA", col("total") * 0.065)
        .otherwise(col("total") * 0.05))

    # Discount tiers — magic numbers
    x = x.withColumn("discount_tier",
        when(col("discount") > 50, 5).when(col("discount") > 35, 4)
        .when(col("discount") > 20, 3).when(col("discount") > 10, 2)
        .when(col("discount") > 0, 1).otherwise(0))

    # Return risk scoring — magic numbers
    x = x.withColumn("return_risk",
        when(col("category") == "electronics", 0.18)
        .when(col("category") == "apparel", 0.25)
        .when(col("category") == "grocery", 0.02)
        .when(col("category") == "home", 0.12)
        .otherwise(0.08))

    # Fraud flags — magic thresholds
    x = x.withColumn("fraud_flag",
        when((col("total") > 5000) & (col("payment_method") == "gift_card"), 3)
        .when((col("total") > 2000) & (col("quantity") > 10), 2)
        .when(col("discount") > 60, 2)
        .when(col("total") > 1000, 1)
        .otherwise(0))

    # Bad variable names
    a = x.filter(col("fraud_flag") >= 2)
    b = x.filter(col("fraud_flag") < 2)
    c = a.count()
    d = b.count()

    # Deep nesting
    if df.count() > 0:
        regions = df.select("region").distinct().collect()
        for r in regions:
            rdf = x.filter(col("region") == r.region)
            if rdf.count() > 0:
                for cat in ["electronics", "apparel", "grocery", "home"]:
                    cdf = rdf.filter(col("category") == cat)
                    if cdf.count() > 50:
                        avg_total = cdf.agg(avg("total")).collect()[0][0]
                        if avg_total > 200:
                            if cdf.filter(col("fraud_flag") >= 2).count() > 5:
                                print(f"ALERT: {r.region}/{cat} avg=${avg_total:.0f}, frauds={cdf.filter(col('fraud_flag') >= 2).count()}")

    # Dead code / unused variables
    unused_config = {"retry": 3, "timeout": 30}
    temp_list = []
    debug_mode = True
    old_formula = None
    deprecated_threshold = 0.65
    legacy_path = "/mnt/legacy/v1/"
    v1_model = None

    return x


# COMMAND ----------

# Save to Delta — collect(), duplication
def SaveToDelta(df):
    df_out = df
    # collect() on large dataset — triggers unnecessary_collect
    all_rows = df.select("txn_id", "total", "fraud_flag").collect()
    for row in all_rows:
        if row.fraud_flag >= 2:
            print(f"FRAUD: Txn {row.txn_id}, Amount=${row.total}")

    # ---- DUPLICATE SAVE 1 ----
    df_out.write.format("delta").mode("overwrite").save("/mnt/delta/retail/transactions")
    print(f"Saved {df_out.count()} transactions")

    # ---- DUPLICATE SAVE 2 ---- (exact copy)
    df_out.write.format("delta").mode("overwrite").save("/mnt/delta/retail/transactions")
    print(f"Saved {df_out.count()} transactions")
    # Dead code / unused variables
    unused_config = {"retry": 3, "timeout": 30}
    temp_list = []
    debug_mode = True

# Execute
spark = SparkSession.builder.appName("RetailSalesETL").getOrCreate()
txns = ExtractTransactions("STORE-NYC-001", "2026-01-01")
cleaned = CleanTransactions(txns, spark.table("ref.products"), spark.table("ref.stores"), spark.table("ref.tax_rules"), spark.table("ref.currency"), spark.table("ref.return_policy"), spark.table("ref.loyalty"), spark.table("mktg.promo_calendar"), spark.table("security.blacklist"), spark.table("security.fraud_rules"), spark.table("ref.geo_mapping"))
SaveToDelta(cleaned)
