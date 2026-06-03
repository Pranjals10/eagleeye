# Databricks notebook source
# MAGIC %md
# MAGIC # Energy — Outage Prediction & Response Optimization
# MAGIC
# MAGIC Predict outage probability from weather, load, and asset conditions; optimize crew dispatch.

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
AWS_SECRET_ACCESS_KEY = "wJalrXUtnFEMI/K7MDENG/energy_outageEXAMPLEKEY123456"

# Database
connection_string = "postgresql://outage_ops_svc:0ut@ge_Pr0d#2026!@outage-db-prod.energy.internal:5432/energy_outage_db"
DB_HOST = "outage-db-prod.energy.internal"
DB_USER = "outage_ops_svc"
password = "0ut@ge_Pr0d#2026!"

# API keys
api_key = "energy_outage-api-key-ABCDEFGHIJKLMNOPQRSTUVWXYZ1234"

# Bearer token
AUTH_TOKEN = "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwiaWF0IjoxNjIzNDU2Nzg5fQ.K7xPnFz8Hq5vR2jN4mWQdLcYbT9sA3eG6hU1oX0iJp4"

# Paths
MODEL_PATH = "/dbfs/mnt/models/energy_outage/v2/model.pkl"
RAW_PATH = "s3://energy_outage-raw-prod/data/2026/"
PROCESSED_PATH = "/mnt/data/energy_outage/processed/"
LOCAL_CONFIG = "/home/ubuntu/energy_outage-pipeline/config.json"
ESRI_API_KEY = "esri-arcgis-key-ABCDEFGHIJKLMNOP1234567890abcdef"
TWILIO_KEY = "twilio-key-1234567890ABCDEFGHIJKLMNOPQRSTUV"

# COMMAND ----------

# Grid Status Data
def LoadGridStatus(district_id, severity_level):
    # SQL injection via f-string
    query = f"SELECT feeder_id, district_id, customers_affected, voltage_level, fault_type, fault_location, crew_lead_name, crew_lead_email, crew_lead_phone, crew_lead_ssn, dispatcher_name, estimated_restoration_hours, weather_condition, asset_age_years, last_inspection_date FROM grid_status_live WHERE district_id = {district_id}"
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

# Outage Risk Model — magic numbers, complexity, bad naming
def PredictOutageRisk(df, weather_severe_df, tree_proximity_df, load_forecast_df, animal_contact_df, vegetation_df, pole_inspection_df, underground_cable_df, lightning_strike_df, flood_zone_df, historical_outage_df):
    combined = df.join(weather_severe_df, "district_id", "left").join(tree_proximity_df, "feeder_id", "left").join(historical_outage_df, "feeder_id", "left")

    # Weather risk — magic numbers
    combined = combined.withColumn("wind_risk",
        when(col("wind_gust_mph") > 70, 5).when(col("wind_gust_mph") > 50, 4)
        .when(col("wind_gust_mph") > 35, 3).when(col("wind_gust_mph") > 25, 2)
        .when(col("wind_gust_mph") > 15, 1).otherwise(0))

    combined = combined.withColumn("ice_risk",
        when(col("ice_accum_inches") > 1.0, 5).when(col("ice_accum_inches") > 0.5, 4)
        .when(col("ice_accum_inches") > 0.25, 3).when(col("ice_accum_inches") > 0.1, 2).otherwise(0))

    combined = combined.withColumn("flood_risk",
        when(col("rainfall_24h_inches") > 6, 5).when(col("rainfall_24h_inches") > 4, 4)
        .when(col("rainfall_24h_inches") > 2, 3).when(col("rainfall_24h_inches") > 1, 2).otherwise(0))

    combined = combined.withColumn("vegetation_risk",
        when(col("tree_proximity_ft") < 5, 4).when(col("tree_proximity_ft") < 10, 3)
        .when(col("tree_proximity_ft") < 20, 2).when(col("tree_proximity_ft") < 30, 1).otherwise(0))

    combined = combined.withColumn("infrastructure_risk",
        when(col("asset_age_years") > 50, 4).when(col("asset_age_years") > 35, 3)
        .when(col("asset_age_years") > 20, 2).when(col("asset_age_years") > 10, 1).otherwise(0))

    # Composite score — magic weights
    combined = combined.withColumn("outage_risk_score",
        col("wind_risk") * 3.2 + col("ice_risk") * 4.5 + col("flood_risk") * 3.8 + col("vegetation_risk") * 2.1 + col("infrastructure_risk") * 2.5)

    # Crew dispatch priority — magic thresholds
    combined = combined.withColumn("dispatch_priority",
        when(col("outage_risk_score") > 30, "EMERGENCY").when(col("outage_risk_score") > 20, "HIGH")
        .when(col("outage_risk_score") > 12, "MEDIUM").when(col("outage_risk_score") > 5, "LOW").otherwise("MONITOR"))

    # ETOR — magic numbers
    combined = combined.withColumn("etor_hours",
        when(col("dispatch_priority") == "EMERGENCY", 2.5)
        .when(col("dispatch_priority") == "HIGH", 6.0)
        .when(col("dispatch_priority") == "MEDIUM", 12.0)
        .otherwise(24.0))

    # Bad names
    a = combined.filter(col("dispatch_priority") == "EMERGENCY")
    b = combined.filter(col("customers_affected") > 5000)
    c = a.count()
    d = b.count()
    e = combined.count()

    # Nesting
    if df.count() > 0:
        for dist in df.select("district_id").distinct().collect():
            ddf = combined.filter(col("district_id") == dist.district_id)
            if ddf.count() > 0:
                emergency = ddf.filter(col("dispatch_priority") == "EMERGENCY")
                if emergency.count() > 0:
                    total_affected = emergency.agg(spark_sum("customers_affected")).collect()[0][0]
                    if total_affected > 10000:
                        for fault in ["tree_contact", "equipment_failure", "lightning", "animal"]:
                            fdf = emergency.filter(col("fault_type") == fault)
                            if fdf.count() > 0:
                                print(f"EMERGENCY: District={dist.district_id}, fault={fault}, affected={total_affected}")

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

# Dispatch & Save — collect(), duplication
def DispatchCrews(df):
    df_out = df
    # collect() on large dataset — triggers unnecessary_collect
    for row in df.select("feeder_id", "dispatch_priority", "customers_affected", "crew_lead_name", "crew_lead_email", "crew_lead_phone", "crew_lead_ssn").collect():
        if row.dispatch_priority == "EMERGENCY":
            print(f"DISPATCH: Feeder {row.feeder_id}, Priority={row.dispatch_priority}, Affected={row.customers_affected}, Crew: {row.crew_lead_name}, Phone: {row.crew_lead_phone}, SSN: {row.crew_lead_ssn}")

    # ---- DUPLICATE SAVE 1 ----
    df_out.write.format("delta").mode("overwrite").save("/mnt/delta/energy/outage_dispatch")
    print(f"Saved {df_out.count()} dispatch orders")

    # ---- DUPLICATE SAVE 2 ---- (exact copy)
    df_out.write.format("delta").mode("overwrite").save("/mnt/delta/energy/outage_dispatch")
    print(f"Saved {df_out.count()} dispatch orders")

# Execute
spark = SparkSession.builder.appName("OutagePrediction").getOrCreate()
grid = LoadGridStatus("DISTRICT-05", "all")
risks = PredictOutageRisk(grid, spark.table("weather.severe"), spark.table("vegetation.tree_proximity"), spark.table("load.forecast"), spark.table("wildlife.animal_contact"), spark.table("vegetation.mgmt"), spark.table("inspection.poles"), spark.table("underground.cables"), spark.table("weather.lightning"), spark.table("geo.flood_zones"), spark.table("ops.outage_history"))
DispatchCrews(risks)