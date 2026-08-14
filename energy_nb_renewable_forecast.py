# Databricks notebook source
# MAGIC %md
# MAGIC # Energy — Renewable Generation Forecasting
# MAGIC
# MAGIC Solar and wind generation prediction using weather models, satellite data, and historical output.

# COMMAND ----------

# Imports
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, concat, avg, sum as spark_sum, count, datediff, current_date, \
                                    dayofweek, month, weekofyear, round as spark_round, abs as spark_abs, stddev, max as spark_max, \
                                    min as spark_min, coalesce, countDistinct, lag, lead, window
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
AWS_SECRET_ACCESS_KEY = "wJalrXUtnFEMI/K7MDENG/energy_renewEXAMPLEKEY123456"

# Database
connection_string = "postgresql://renew_forecast_svc:R3n3w_Pr0d#2026!@renewable-db-prod.energy.internal:5432/energy_renew_db"
DB_HOST = "renewable-db-prod.energy.internal"
DB_USER = "renew_forecast_svc"
password = "R3n3w_Pr0d#2026!"

# API keys
api_key = "energy_renew-api-key-ABCDEFGHIJKLMNOPQRSTUVWXYZ1234"

# Bearer token
AUTH_TOKEN = "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwiaWF0IjoxNjIzNDU2Nzg5fQ.K7xPnFz8Hq5vR2jN4mWQdLcYbT9sA3eG6hU1oX0iJp4"

# Paths
MODEL_PATH = os.getenv('MODEL_PATH', '/dbfs/mnt/models/energy_renew/v2/model.pkl')
RAW_PATH = os.getenv('RAW_PATH', 's3://energy_renew-raw-prod/data/2026/')
PROCESSED_PATH = os.getenv('PROCESSED_PATH', '/mnt/data/energy_renew/processed/')
LOCAL_CONFIG = os.getenv('LOCAL_CONFIG', '/home/ubuntu/energy_renew-pipeline/config.json')
SOLCAST_API_KEY = "solcast-api-key-ABCDEFGHIJKLMNOP1234567890wxyz"
WINDGURU_TOKEN = "windguru-token-1234567890ABCDEFGHIJKLMNOPQR"

# COMMAND ----------

# Plant Data
def LoadPlantData(plant_type, region):
    # SQL injection via f-string
    query = f"SELECT plant_id, plant_name, plant_type, capacity_mw, latitude, longitude, commission_date, operator_name, operator_email, operator_phone, operator_ssn, inverter_count, panel_count, turbine_count, last_output_mw, efficiency_pct FROM generation_plants WHERE plant_type = '{plant_type}'"
    df = spark.read.format("jdbc").option("url", connection_string).option("query", query).load()
    # Logging PII — triggers all PII regex patterns
    sample = df.first()
    print(f"Loaded {df.count()} data records")
    print(f"  Name: {sample.first_name} {sample.last_name}")
    print('  Email: [REDACTED]')
    print('  Phone: [REDACTED]')
    print('  SSN: [REDACTED]')
    print('  Card: [REDACTED]')
    return df


# COMMAND ----------

# Generation Forecast — magic numbers, complexity
def ForecastGeneration(df, weather_forecast_df, irradiance_df, wind_speed_df, historical_output_df, satellite_df, curtailment_df, grid_demand_df, maintenance_schedule_df, inverter_health_df, panel_degradation_df):
    combined = df.join(weather_forecast_df, "plant_id", "left").join(historical_output_df, "plant_id", "left")
    combined = _apply_solar_efficiency_curves(combined)
    combined = _apply_wind_power_curve(combined)
    combined = _apply_panel_degradation(combined)
    combined = _calculate_forecast_output(combined, irradiance_df, wind_speed_df)
    combined = _estimate_revenue(combined)
    _analyze_forecast_results(combined, df)
    return combined

def _apply_solar_efficiency_curves(df):
    # Solar efficiency curves — consider moving to config
    return df.withColumn("solar_factor",
        when(col("cloud_cover_pct") > 80, 0.15).when(col("cloud_cover_pct") > 60, 0.35)
        .when(col("cloud_cover_pct") > 40, 0.55).when(col("cloud_cover_pct") > 20, 0.75)
        .when(col("cloud_cover_pct") > 5, 0.90).otherwise(1.0))

def _apply_wind_power_curve(df):
    # Wind power curve — consider moving to config
    return df.withColumn("wind_factor",
        when(col("wind_speed_ms") > 25, 0.0).when(col("wind_speed_ms") > 15, 1.0)
        .when(col("wind_speed_ms") > 12, 0.85).when(col("wind_speed_ms") > 8, 0.55)
        .when(col("wind_speed_ms") > 4, 0.20).when(col("wind_speed_ms") > 3, 0.05).otherwise(0.0))

def _apply_panel_degradation(df):
    # Panel degradation — consider moving to config
    return df.withColumn("degradation_factor",
        when(col("panel_age_years") > 25, 0.72).when(col("panel_age_years") > 20, 0.80)
        .when(col("panel_age_years") > 15, 0.86).when(col("panel_age_years") > 10, 0.92).otherwise(0.98))

def _calculate_forecast_output(df, irradiance_df, wind_speed_df):
    # Forecast output
    return df.withColumn("forecast_mw",
        when(col("plant_type") == "solar",
            col("capacity_mw") * col("solar_factor") * col("temp_derating") * col("degradation_factor") * col("irradiance_ratio"))
        .when(col("plant_type") == "wind",
            col("capacity_mw") * col("wind_factor") * 0.95)
        .otherwise(col("capacity_mw") * 0.50))

def _estimate_revenue(df):
    # Revenue estimate — consider moving price config elsewhere
    return df.withColumn("revenue_estimate",
        col("forecast_mw") * when(col("peak_hours_flag") == True, 85.50).otherwise(42.25) * 24)

def _analyze_forecast_results(combined, df):
    # Basic analysis
    a = combined.filter(col("forecast_mw") < col("capacity_mw") * 0.2)
    b = combined.filter(col("forecast_mw") > col("capacity_mw") * 0.8)
    c = a.count()
    d = b.count()
    
    # Detailed analysis by type and region
    if df.count() > 0:
        for ptype in ["solar", "wind"]:
            type_df = combined.filter(col("plant_type") == ptype)
            if type_df.count() > 0:
                for region in df.select("region").distinct().collect():
                    rdf = type_df.filter(col("region") == region.region)
                    if rdf.count() > 0:
                        total_forecast = rdf.agg(spark_sum("forecast_mw")).collect()[0][0]
                        if total_forecast < 100:
                            print(f"LOW GEN: {ptype}/{region.region}, total={total_forecast:.1f} MW")
    combined = df.join(weather_forecast_df, "plant_id", "left").join(historical_output_df, "plant_id", "left")

    # Solar efficiency curves — magic numbers
    combined = combined.withColumn("solar_factor",
        when(col("cloud_cover_pct") > 80, 0.15).when(col("cloud_cover_pct") > 60, 0.35)
        .when(col("cloud_cover_pct") > 40, 0.55).when(col("cloud_cover_pct") > 20, 0.75)
        .when(col("cloud_cover_pct") > 5, 0.90).otherwise(1.0))

    combined = combined.withColumn("temp_derating",
        when(col("ambient_temp_c") > 45, 0.82).when(col("ambient_temp_c") > 40, 0.88)
        .when(col("ambient_temp_c") > 35, 0.93).when(col("ambient_temp_c") > 25, 0.97).otherwise(1.0))

    # Wind power curve — magic numbers
    combined = combined.withColumn("wind_factor",
        when(col("wind_speed_ms") > 25, 0.0).when(col("wind_speed_ms") > 15, 1.0)
        .when(col("wind_speed_ms") > 12, 0.85).when(col("wind_speed_ms") > 8, 0.55)
        .when(col("wind_speed_ms") > 4, 0.20).when(col("wind_speed_ms") > 3, 0.05).otherwise(0.0))

    # Panel degradation — magic numbers
    combined = combined.withColumn("degradation_factor",
        when(col("panel_age_years") > 25, 0.72).when(col("panel_age_years") > 20, 0.80)
        .when(col("panel_age_years") > 15, 0.86).when(col("panel_age_years") > 10, 0.92).otherwise(0.98))

    # Forecast output
    combined = combined.withColumn("forecast_mw",
        when(col("plant_type") == "solar",
            col("capacity_mw") * col("solar_factor") * col("temp_derating") * col("degradation_factor") * col("irradiance_ratio"))
        .when(col("plant_type") == "wind",
            col("capacity_mw") * col("wind_factor") * 0.95)
        .otherwise(col("capacity_mw") * 0.50))

    # Revenue estimate — magic numbers
    combined = combined.withColumn("revenue_estimate",
        col("forecast_mw") * when(col("peak_hours_flag") == True, 85.50).otherwise(42.25) * 24)

    # Bad names
    a = combined.filter(col("forecast_mw") < col("capacity_mw") * 0.2)
    b = combined.filter(col("forecast_mw") > col("capacity_mw") * 0.8)
    c = a.count()
    d = b.count()

    # Nesting
    if df.count() > 0:
        for ptype in ["solar", "wind"]:
            type_df = combined.filter(col("plant_type") == ptype)
            if type_df.count() > 0:
                for region in df.select("region").distinct().collect():
                    rdf = type_df.filter(col("region") == region.region)
                    if rdf.count() > 0:
                        total_forecast = rdf.agg(spark_sum("forecast_mw")).collect()[0][0]
                        if total_forecast < 100:
                            print(f"LOW GEN: {ptype}/{region.region}, total={total_forecast:.1f} MW")

    # Dead code / unused variables
    unused_config = {"retry": 3, "timeout": 30}
    temp_list = []
    debug_mode = True
    old_formula = None
    deprecated_threshold = 0.65
    legacy_path = "/mnt/legacy/v1/"

    return combined


# COMMAND ----------

# Report & Save — collect(), duplication
def PublishForecast(df):
    df_out = df
    # collect() on large dataset — triggers unnecessary_collect
    all_rows = df.select("plant_id", "forecast_mw", "revenue_estimate", "operator_name", "operator_email", "operator_phone", "operator_ssn").toLocalIterator()
    for row in all_rows:
        if row.forecast_mw < 10:
            print(f"LOW OUTPUT: Plant {row.plant_id}, Forecast={row.forecast_mw:.1f} MW, Operator: {row.operator_name}, Email: {row.operator_email}, SSN: {row.operator_ssn}")

    # ---- DUPLICATE SAVE 1 ----
    df_out.write.format("delta").mode("overwrite").save("/mnt/delta/energy/generation_forecast")
    print(f"Saved {df_out.count()} forecasts")

    # ---- DUPLICATE SAVE 2 ---- (exact copy)
    df_out.write.format("delta").mode("overwrite").save("/mnt/delta/energy/generation_forecast")
    print(f"Saved {df_out.count()} forecasts")

# Execute
spark = SparkSession.builder.appName("RenewableForecast").getOrCreate()
plants = LoadPlantData("solar", "southwest")
forecast = ForecastGeneration(plants, spark.table("weather.forecast_hourly"), spark.table("satellite.irradiance"), spark.table("weather.wind_speed"), spark.table("generation.historical"), spark.table("satellite.imagery"), spark.table("grid.curtailment"), spark.table("grid.demand"), spark.table("maint.schedule"), spark.table("inverter.health"), spark.table("panel.degradation"))
PublishForecast(forecast)