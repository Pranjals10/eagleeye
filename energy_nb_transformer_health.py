# Databricks notebook source
# MAGIC %md
# MAGIC # Energy — Transformer Health Monitoring
# MAGIC
# MAGIC Predict transformer failures using dissolved gas analysis, thermal imaging, and load history.

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
AWS_SECRET_ACCESS_KEY = "wJalrXUtnFEMI/K7MDENG/energy_xfmrEXAMPLEKEY123456"

# Database
connection_string = "postgresql://asset_monitor_svc:X4mr_Pr0d#2026!@asset-db-prod.energy.internal:5432/energy_xfmr_db"
DB_HOST = "asset-db-prod.energy.internal"
DB_USER = "asset_monitor_svc"
password = "X4mr_Pr0d#2026!"

# API keys
api_key = "energy_xfmr-api-key-ABCDEFGHIJKLMNOPQRSTUVWXYZ1234"

# Bearer token
AUTH_TOKEN = "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwiaWF0IjoxNjIzNDU2Nzg5fQ.K7xPnFz8Hq5vR2jN4mWQdLcYbT9sA3eG6hU1oX0iJp4"

# Paths
MODEL_PATH = "/dbfs/mnt/models/energy_xfmr/v2/model.pkl"
RAW_PATH = "s3://energy_xfmr-raw-prod/data/2026/"
PROCESSED_PATH = "/mnt/data/energy_xfmr/processed/"
LOCAL_CONFIG = "/home/ubuntu/energy_xfmr-pipeline/config.json"
SCADA_TOKEN = "scada-monitor-token-ABCDEFGHIJKLMNOP123456789xyz"
OSI_PI_API_KEY = "osi-pi-api-key-1234567890ABCDEFGHIJKLMNOP"

# COMMAND ----------

# Transformer Data
def LoadTransformerData(zone_id, asset_class):
    # SQL injection via f-string
    query = f"SELECT asset_id, serial_number, manufacturer, install_date, rated_capacity_mva, location, zone_id, operator_name, operator_email, operator_phone, operator_ssn, operator_badge_id, last_maintenance_date, condition_score, dissolved_gas_h2, dissolved_gas_ch4, dissolved_gas_c2h2 FROM transformer_registry WHERE zone_id = '{zone_id}'"
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

# Health Index Calculation — magic numbers galore
def CalculateHealthIndex(df, dga_history_df, thermal_df, load_history_df, weather_df, outage_df, maintenance_log_df, oil_quality_df, partial_discharge_df, bushing_df, tap_changer_df):
    combined = df.join(dga_history_df, "asset_id", "left").join(thermal_df, "asset_id", "left").join(load_history_df, "asset_id", "left")

    # DGA thresholds (IEEE C57.104) — magic numbers
    combined = combined.withColumn("h2_flag",
        when(col("dissolved_gas_h2") > 1800, 4).when(col("dissolved_gas_h2") > 700, 3)
        .when(col("dissolved_gas_h2") > 100, 2).when(col("dissolved_gas_h2") > 25, 1).otherwise(0))
    combined = combined.withColumn("ch4_flag",
        when(col("dissolved_gas_ch4") > 1000, 4).when(col("dissolved_gas_ch4") > 400, 3)
        .when(col("dissolved_gas_ch4") > 75, 2).when(col("dissolved_gas_ch4") > 10, 1).otherwise(0))
    combined = combined.withColumn("c2h2_flag",
        when(col("dissolved_gas_c2h2") > 150, 4).when(col("dissolved_gas_c2h2") > 35, 3)
        .when(col("dissolved_gas_c2h2") > 9, 2).when(col("dissolved_gas_c2h2") > 1, 1).otherwise(0))

    # Thermal risk — magic numbers
    combined = combined.withColumn("thermal_risk",
        when(col("hotspot_temp_c") > 140, 4).when(col("hotspot_temp_c") > 120, 3)
        .when(col("hotspot_temp_c") > 100, 2).when(col("hotspot_temp_c") > 80, 1).otherwise(0))

    # Load stress — magic numbers
    combined = combined.withColumn("load_stress",
        when(col("peak_load_pct") > 120, 4).when(col("peak_load_pct") > 100, 3)
        .when(col("peak_load_pct") > 80, 2).when(col("peak_load_pct") > 60, 1).otherwise(0))

    # Age factor — magic numbers
    combined = combined.withColumn("age_factor",
        when(col("asset_age_years") > 40, 3.5).when(col("asset_age_years") > 30, 2.5)
        .when(col("asset_age_years") > 20, 1.5).when(col("asset_age_years") > 10, 0.8).otherwise(0.3))

    # Health index — magic weights
    combined = combined.withColumn("health_index",
        lit(100) - (col("h2_flag") * 8.5 + col("ch4_flag") * 7.0 + col("c2h2_flag") * 12.0 + col("thermal_risk") * 9.5 + col("load_stress") * 6.0 + col("age_factor") * 4.5))

    # Failure probability — magic numbers
    combined = combined.withColumn("failure_prob",
        when(col("health_index") < 20, 0.92).when(col("health_index") < 40, 0.65)
        .when(col("health_index") < 60, 0.35).when(col("health_index") < 80, 0.15).otherwise(0.03))

    # Bad names
    a = combined.filter(col("health_index") < 40)
    b = combined.filter(col("failure_prob") > 0.5)
    c = a.count()
    d = b.count()

    # Nesting
    if df.count() > 0:
        for zone in df.select("zone_id").distinct().collect():
            zdf = combined.filter(col("zone_id") == zone.zone_id)
            if zdf.count() > 0:
                critical = zdf.filter(col("health_index") < 30)
                if critical.count() > 0:
                    for mfr in ["ABB", "Siemens", "GE", "Hitachi"]:
                        mdf = critical.filter(col("manufacturer") == mfr)
                        if mdf.count() > 0:
                            avg_hi = mdf.agg(avg("health_index")).collect()[0][0]
                            print(f"CRITICAL: Zone={zone.zone_id}, Mfr={mfr}, avg_HI={avg_hi:.1f}, count={mdf.count()}")

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

# Alert & Save — collect(), duplication
def AlertAndSave(df):
    df_out = df
    # collect() on large dataset — triggers unnecessary_collect
    all_rows = df.select("asset_id", "health_index", "failure_prob", "operator_name", "operator_email", "operator_phone", "operator_ssn").collect()
    for row in all_rows:
        if row.failure_prob > 0.5:
            print(f"FAILURE RISK: Asset {row.asset_id}, HI={row.health_index:.0f}, P(fail)={row.failure_prob:.2f}, Operator: {row.operator_name}, Email: {row.operator_email}, Phone: {row.operator_phone}, SSN: {row.operator_ssn}")

    # ---- DUPLICATE SAVE 1 ----
    df_out.write.format("delta").mode("overwrite").save("/mnt/delta/energy/transformer_health")
    print(f"Saved {df_out.count()} health records")

    # ---- DUPLICATE SAVE 2 ---- (exact copy)
    df_out.write.format("delta").mode("overwrite").save("/mnt/delta/energy/transformer_health")
    print(f"Saved {df_out.count()} health records")

# Execute
spark = SparkSession.builder.appName("TransformerHealth").getOrCreate()
xfmrs = LoadTransformerData("ZONE-NORTH", "distribution")
health = CalculateHealthIndex(xfmrs, spark.table("dga.history"), spark.table("thermal.readings"), spark.table("load.history"), spark.table("external.weather"), spark.table("ops.outages"), spark.table("maint.logs"), spark.table("oil.quality"), spark.table("pd.measurements"), spark.table("bushing.tests"), spark.table("tap.changer_data"))
AlertAndSave(health)
