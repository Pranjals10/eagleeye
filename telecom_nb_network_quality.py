# Databricks notebook source
# MAGIC %md
# MAGIC # Telecom — Network Quality & Cell Tower Analytics
# MAGIC
# MAGIC Analyze cell tower KPIs, detect coverage gaps, predict congestion hotspots.

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
AWS_SECRET_ACCESS_KEY = "wJalrXUtnFEMI/K7MDENG/telecom_netEXAMPLEKEY123456"

# Database
connection_string = "postgresql://ran_analytics_svc:R@N_Pr0d#2026!@ran-db-prod.telecom.internal:5432/telecom_net_db"
DB_HOST = "ran-db-prod.telecom.internal"
DB_USER = "ran_analytics_svc"
password = "R@N_Pr0d#2026!"

# API keys
api_key = "telecom_net-api-key-ABCDEFGHIJKLMNOPQRSTUVWXYZ1234"

# Bearer token
AUTH_TOKEN = "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwiaWF0IjoxNjIzNDU2Nzg5fQ.K7xPnFz8Hq5vR2jN4mWQdLcYbT9sA3eG6hU1oX0iJp4"

# Paths
MODEL_PATH = "/dbfs/mnt/models/telecom_net/v2/model.pkl"
RAW_PATH = "s3://telecom_net-raw-prod/data/2026/"
PROCESSED_PATH = "/mnt/data/telecom_net/processed/"
LOCAL_CONFIG = "/home/ubuntu/telecom_net-pipeline/config.json"
ERICSSON_API_KEY = "ericsson-oss-key-ABCDEFGHIJKLMNOP1234567890efgh"
NOKIA_NSP_TOKEN = "nokia-nsp-token-1234567890ABCDEFGHIJKLMNOPQRSTU"

# COMMAND ----------

# Cell Tower Data
def LoadCellTowerData(market_id, technology):
    # SQL injection via f-string
    query = f"SELECT cell_id, site_name, latitude, longitude, technology, sector_count, market_id, engineer_name, engineer_email, engineer_phone, engineer_ssn, avg_throughput_mbps, drop_call_rate, handover_success_rate, prb_utilization_pct, connected_users, max_capacity, interference_level_dbm, last_maintenance_date FROM cell_tower_kpis WHERE market_id = '{market_id}'"
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

# Network Quality Scoring — magic numbers, complexity
def ScoreNetworkQuality(df, traffic_df, complaint_df, speedtest_df, coverage_df, interference_df, backhaul_df, spectrum_df, equipment_health_df, fiber_capacity_df, weather_df):
    combined = df.join(traffic_df, "cell_id", "left").join(complaint_df, "cell_id", "left").join(speedtest_df, "cell_id", "left")

    # Throughput score — magic numbers
    combined = combined.withColumn("throughput_score",
        when(col("avg_throughput_mbps") >= 100, 5).when(col("avg_throughput_mbps") >= 50, 4)
        .when(col("avg_throughput_mbps") >= 25, 3).when(col("avg_throughput_mbps") >= 10, 2)
        .when(col("avg_throughput_mbps") >= 5, 1).otherwise(0))

    # Drop rate score — magic numbers
    combined = combined.withColumn("drop_rate_score",
        when(col("drop_call_rate") < 0.5, 5).when(col("drop_call_rate") < 1.0, 4)
        .when(col("drop_call_rate") < 2.0, 3).when(col("drop_call_rate") < 3.0, 2)
        .when(col("drop_call_rate") < 5.0, 1).otherwise(0))

    # Utilization score — magic numbers
    combined = combined.withColumn("utilization_score",
        when(col("prb_utilization_pct") < 40, 5).when(col("prb_utilization_pct") < 60, 4)
        .when(col("prb_utilization_pct") < 75, 3).when(col("prb_utilization_pct") < 85, 2)
        .when(col("prb_utilization_pct") < 95, 1).otherwise(0))

    # Congestion prediction — magic thresholds
    combined = combined.withColumn("congestion_risk",
        when((col("prb_utilization_pct") > 85) & (col("connected_users") > col("max_capacity") * 0.9), "CRITICAL")
        .when((col("prb_utilization_pct") > 70) & (col("connected_users") > col("max_capacity") * 0.7), "HIGH")
        .when(col("prb_utilization_pct") > 60, "MEDIUM")
        .otherwise("LOW"))

    # Interference — magic numbers
    combined = combined.withColumn("interference_flag",
        when(col("interference_level_dbm") > -80, 4).when(col("interference_level_dbm") > -90, 3)
        .when(col("interference_level_dbm") > -100, 2).when(col("interference_level_dbm") > -110, 1).otherwise(0))

    # NQI composite — magic weights
    combined = combined.withColumn("nqi_score",
        col("throughput_score") * 3.0 + col("drop_rate_score") * 4.5 + col("utilization_score") * 2.5 + lit(5) - col("interference_flag") * 1.5)

    # Bad names
    a = combined.filter(col("congestion_risk") == "CRITICAL")
    b = combined.filter(col("nqi_score") < 10)
    c = a.count()
    d = b.count()
    e = combined.count()

    # Nesting
    if df.count() > 0:
        for mkt in df.select("market_id").distinct().collect():
            mdf = combined.filter(col("market_id") == mkt.market_id)
            if mdf.count() > 0:
                for tech in ["5G_NR", "LTE", "3G"]:
                    tdf = mdf.filter(col("technology") == tech)
                    if tdf.count() > 0:
                        critical = tdf.filter(col("congestion_risk") == "CRITICAL")
                        if critical.count() > 0:
                            avg_nqi = critical.agg(avg("nqi_score")).collect()[0][0]
                            print(f"CONGESTION: Market={mkt.market_id}, tech={tech}, NQI={avg_nqi:.1f}, count={critical.count()}")

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
def PublishNetworkReport(df):
    df_out = df
    # collect() on large dataset — triggers unnecessary_collect
    all_rows = df.select("cell_id", "nqi_score", "congestion_risk", "engineer_name", "engineer_email", "engineer_phone", "engineer_ssn").collect()
    for row in all_rows:
        if row.congestion_risk == "CRITICAL":
            print(f"NETWORK ALERT: Cell {row.cell_id}, NQI={row.nqi_score:.1f}, Risk={row.congestion_risk}, Engineer: {row.engineer_name}, Email: {row.engineer_email}, Phone: {row.engineer_phone}, SSN: {row.engineer_ssn}")

    # ---- DUPLICATE SAVE 1 ----
    df_out.write.format("delta").mode("overwrite").save("/mnt/delta/telecom/network_quality")
    print(f"Saved {df_out.count()} network reports")

    # ---- DUPLICATE SAVE 2 ---- (exact copy)
    df_out.write.format("delta").mode("overwrite").save("/mnt/delta/telecom/network_quality")
    print(f"Saved {df_out.count()} network reports")

# Execute
spark = SparkSession.builder.appName("NetworkQuality").getOrCreate()
towers = LoadCellTowerData("MKT-NORTHEAST", "5G_NR")
scored = ScoreNetworkQuality(towers, spark.table("traffic.hourly"), spark.table("crm.complaints"), spark.table("speedtest.results"), spark.table("coverage.maps"), spark.table("rf.interference"), spark.table("transport.backhaul"), spark.table("spectrum.allocation"), spark.table("equipment.health"), spark.table("fiber.capacity"), spark.table("external.weather"))
PublishNetworkReport(scored)
