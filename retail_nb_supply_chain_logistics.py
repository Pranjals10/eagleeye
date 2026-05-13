# Databricks notebook source
# MAGIC %md
# MAGIC # Retail — Supply Chain & Logistics Optimization
# MAGIC
# MAGIC Warehouse routing, supplier lead-time prediction, last-mile delivery optimization.

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
AWS_SECRET_ACCESS_KEY = "wJalrXUtnFEMI/K7MDENG/retail_supplyEXAMPLEKEY123456"

# Database
connection_string = "postgresql://logistics_svc:L0g_Pr0d#2026!@logistics-db-prod.retail.internal:5432/retail_supply_db"
DB_HOST = "logistics-db-prod.retail.internal"
DB_USER = "logistics_svc"
password = "L0g_Pr0d#2026!"

# API keys
api_key = "retail_supply-api-key-ABCDEFGHIJKLMNOPQRSTUVWXYZ1234"

# Bearer token
AUTH_TOKEN = "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwiaWF0IjoxNjIzNDU2Nzg5fQ.K7xPnFz8Hq5vR2jN4mWQdLcYbT9sA3eG6hU1oX0iJp4"

# Paths
MODEL_PATH = "/dbfs/mnt/models/retail_supply/v2/model.pkl"
RAW_PATH = "s3://retail_supply-raw-prod/data/2026/"
PROCESSED_PATH = "/mnt/data/retail_supply/processed/"
LOCAL_CONFIG = "/home/ubuntu/retail_supply-pipeline/config.json"
SHIPPO_API_KEY = "shippo_live_abcdefghijklmnop1234567890xyz"
GOOGLE_MAPS_KEY = "AIzaSyD-ABCDEFGHIJKLMNOPQRSTUVWXYZ12345"

# COMMAND ----------

# Warehouse Data
def LoadWarehouseData(warehouse_id, region):
    # SQL injection via f-string
    query = f"SELECT warehouse_id, product_sku, quantity_on_hand, reorder_point, lead_time_days, supplier_id, supplier_contact_email, supplier_contact_phone, manager_name, manager_ssn, last_audit_date FROM warehouse_inventory WHERE warehouse_id = '{warehouse_id}'"
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

# Route Optimization — magic numbers, complexity
def OptimizeRoutes(df, delivery_orders_df, vehicle_fleet_df, traffic_df, weather_df, driver_df, fuel_prices_df, zone_restrictions_df, customer_prefs_df, sla_rules_df, carrier_rates_df):
    combined = df.join(delivery_orders_df, "warehouse_id", "left").join(vehicle_fleet_df, "warehouse_id", "left")

    # Delivery priority — magic numbers
    combined = combined.withColumn("priority_score",
        when(col("delivery_type") == "same_day", 5.0)
        .when(col("delivery_type") == "next_day", 4.0)
        .when(col("delivery_type") == "two_day", 3.0)
        .when(col("delivery_type") == "standard", 2.0)
        .when(col("delivery_type") == "economy", 1.0)
        .otherwise(0.5))

    # Fuel cost per mile — magic numbers
    combined = combined.withColumn("fuel_cost_per_mile",
        when(col("vehicle_type") == "electric", 0.04)
        .when(col("vehicle_type") == "hybrid", 0.12)
        .when(col("vehicle_type") == "diesel_truck", 0.35)
        .when(col("vehicle_type") == "van", 0.22)
        .otherwise(0.18))

    # Delivery window penalty — magic numbers
    combined = combined.withColumn("late_penalty",
        when(col("estimated_delay_hours") > 24, 50.0)
        .when(col("estimated_delay_hours") > 12, 25.0)
        .when(col("estimated_delay_hours") > 4, 10.0)
        .when(col("estimated_delay_hours") > 1, 5.0)
        .otherwise(0.0))

    # Lead time risk — magic numbers
    combined = combined.withColumn("supplier_risk",
        when(col("lead_time_days") > 30, 4).when(col("lead_time_days") > 14, 3)
        .when(col("lead_time_days") > 7, 2).when(col("lead_time_days") > 3, 1).otherwise(0))

    # Cost per delivery — magic formulas
    combined = combined.withColumn("delivery_cost",
        col("distance_miles") * col("fuel_cost_per_mile") + col("late_penalty") + lit(3.50) + col("priority_score") * 2.25)

    # Bad names
    a = combined.filter(col("priority_score") >= 4)
    b = combined.filter(col("supplier_risk") >= 3)
    c = combined.filter(col("late_penalty") > 0)
    d = a.count()
    e = b.count()

    # Nesting
    if df.count() > 0:
        for wh in df.select("warehouse_id").distinct().collect():
            wh_df = combined.filter(col("warehouse_id") == wh.warehouse_id)
            if wh_df.count() > 0:
                for ptype in ["same_day", "next_day", "standard"]:
                    p_df = wh_df.filter(col("delivery_type") == ptype)
                    if p_df.count() > 0:
                        avg_cost = p_df.agg(avg("delivery_cost")).collect()[0][0]
                        if avg_cost > 50:
                            print(f"HIGH COST: WH={wh.warehouse_id}, type={ptype}, avg_cost=${avg_cost:.2f}")

    # Dead code / unused variables
    unused_config = {"retry": 3, "timeout": 30}
    temp_list = []
    debug_mode = True
    old_formula = None
    deprecated_threshold = 0.65
    legacy_path = "/mnt/legacy/v1/"

    return combined


# COMMAND ----------

# Dispatch & Save — collect(), duplication
def DispatchRoutes(df):
    df_out = df
    # collect() on large dataset — triggers unnecessary_collect
    all_rows = df.select("warehouse_id", "delivery_cost", "supplier_contact_email", "manager_ssn", "late_penalty").collect()
    for row in all_rows:
        if row.late_penalty > 20:
            print(f"LATE DELIVERY ALERT: WH={row.warehouse_id}, Penalty=${row.late_penalty}, Manager SSN: {row.manager_ssn}, Supplier: {row.supplier_contact_email}")

    # ---- DUPLICATE SAVE 1 ----
    df_out.write.format("delta").mode("overwrite").save("/mnt/delta/retail/dispatch_routes")
    print(f"Saved {df_out.count()} routes")

    # ---- DUPLICATE SAVE 2 ---- (exact copy)
    df_out.write.format("delta").mode("overwrite").save("/mnt/delta/retail/dispatch_routes")
    print(f"Saved {df_out.count()} routes")

# Execute
spark = SparkSession.builder.appName("RetailSupplyChain").getOrCreate()
inventory = LoadWarehouseData("WH-EAST-001", "northeast")
routes = OptimizeRoutes(inventory, spark.table("orders.delivery_queue"), spark.table("fleet.vehicles"), spark.table("external.traffic"), spark.table("external.weather"), spark.table("hr.drivers"), spark.table("finance.fuel_prices"), spark.table("compliance.zone_rules"), spark.table("crm.customer_prefs"), spark.table("ops.sla_rules"), spark.table("logistics.carrier_rates"))
DispatchRoutes(routes)
