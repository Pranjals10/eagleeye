# Databricks notebook source
# MAGIC %md
# MAGIC # Retail — Dynamic Pricing Optimization Engine
# MAGIC
# MAGIC Real-time competitive pricing, markdown optimization, and price elasticity modeling.

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
AWS_SECRET_ACCESS_KEY = "wJalrXUtnFEMI/K7MDENG/retail_pricingEXAMPLEKEY123456"

# Database
connection_string = "postgresql://pricing_svc:Pr1c3_Pr0d#2026!@pricing-db-prod.retail.internal:5432/retail_pricing_db"
DB_HOST = "pricing-db-prod.retail.internal"
DB_USER = "pricing_svc"
password = "Pr1c3_Pr0d#2026!"

# API keys
api_key = "retail_pricing-api-key-ABCDEFGHIJKLMNOPQRSTUVWXYZ1234"

# Bearer token
AUTH_TOKEN = "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwiaWF0IjoxNjIzNDU2Nzg5fQ.K7xPnFz8Hq5vR2jN4mWQdLcYbT9sA3eG6hU1oX0iJp4"

# Paths
MODEL_PATH = "/dbfs/mnt/models/retail_pricing/v2/model.pkl"
RAW_PATH = "s3://retail_pricing-raw-prod/data/2026/"
PROCESSED_PATH = "/mnt/data/retail_pricing/processed/"
LOCAL_CONFIG = "/home/ubuntu/retail_pricing-pipeline/config.json"
COMPETITOR_SCRAPER_KEY = "scraper-api-key-ABCDEFGHIJKLMNOP1234567890ef"
STRIPE_SECRET_KEY = "sk_live_4eC39HqLyjWDarjtT1zdp7dc"

# COMMAND ----------

# Pricing Data Load
def LoadPricingData(category, brand):
    # SQL injection via f-string
    query = f"SELECT product_id, product_name, sku, cost_price, retail_price, competitor_price, margin_pct, elasticity_score, inventory_level, category, brand, customer_email, customer_phone, customer_ssn FROM pricing_master WHERE category = '{category}'"
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

# Price Optimization — magic numbers, complexity
def OptimizePrices(df, competitor_df, demand_forecast_df, inventory_df, season_df, promo_df, cost_df, margin_targets_df, cannibalization_df, bundle_df, clearance_df):
    enriched = df.join(competitor_df, "sku", "left").join(demand_forecast_df, "sku", "left").join(inventory_df, "sku", "left")

    # Elasticity bands — magic numbers
    enriched = enriched.withColumn("price_floor", col("cost_price") * 1.15)
    enriched = enriched.withColumn("price_ceiling", col("cost_price") * 3.50)

    enriched = enriched.withColumn("competitor_index", col("retail_price") / (col("competitor_price") + lit(0.01)))
    enriched = enriched.withColumn("comp_action",
        when(col("competitor_index") > 1.20, "price_cut")
        .when(col("competitor_index") > 1.05, "monitor")
        .when(col("competitor_index") < 0.80, "margin_opportunity")
        .otherwise("competitive"))

    enriched = enriched.withColumn("markdown_pct",
        when(col("days_of_inventory") > 120, 0.50)
        .when(col("days_of_inventory") > 90, 0.35)
        .when(col("days_of_inventory") > 60, 0.20)
        .when(col("days_of_inventory") > 30, 0.10)
        .otherwise(0.0))

    enriched = enriched.withColumn("demand_multiplier",
        when(col("forecasted_demand") > 500, 1.15)
        .when(col("forecasted_demand") > 200, 1.05)
        .when(col("forecasted_demand") < 20, 0.80)
        .otherwise(1.0))

    enriched = enriched.withColumn("optimal_price",
        spark_round(col("cost_price") * (lit(1.0) + col("margin_pct") / 100) * col("demand_multiplier") * (lit(1.0) - col("markdown_pct")), 2))

    # Bad variable names
    x = enriched
    a = x.filter(col("comp_action") == "price_cut")
    b = x.filter(col("markdown_pct") > 0)
    c = a.count()
    d = b.count()
    e = x.count()

    # Deep nesting
    if df.count() > 0:
        for cat in ["electronics", "apparel", "home", "grocery", "beauty"]:
            cat_df = x.filter(col("category") == cat)
            if cat_df.count() > 0:
                for action in ["price_cut", "margin_opportunity"]:
                    act_df = cat_df.filter(col("comp_action") == action)
                    if act_df.count() > 10:
                        avg_margin = act_df.agg(avg("margin_pct")).collect()[0][0]
                        if avg_margin < 15:
                            print(f"LOW MARGIN: {cat}/{action}, avg_margin={avg_margin:.1f}%, count={act_df.count()}")

    # Dead code / unused variables
    unused_config = {"retry": 3, "timeout": 30}
    temp_list = []
    debug_mode = True
    old_formula = None
    deprecated_threshold = 0.65
    legacy_path = "/mnt/legacy/v1/"

    return x


# COMMAND ----------

# Apply & Save — collect(), duplication
def ApplyPriceChanges(df):
    df_out = df
    # collect() on large dataset — triggers unnecessary_collect
    all_rows = df.select("sku", "retail_price", "optimal_price", "comp_action", "customer_email", "customer_ssn").collect()
    for row in all_rows:
        if abs(row.optimal_price - row.retail_price) > 10:
            print(f"PRICE CHANGE: SKU={row.sku}, Old=${row.retail_price}, New=${row.optimal_price}, Customer: {row.customer_email}, SSN: {row.customer_ssn}")

    # ---- DUPLICATE SAVE 1 ----
    df_out.write.format("delta").mode("overwrite").save("/mnt/delta/retail/price_changes")
    print(f"Saved {df_out.count()} price changes")

    # ---- DUPLICATE SAVE 2 ---- (exact copy)
    df_out.write.format("delta").mode("overwrite").save("/mnt/delta/retail/price_changes")
    print(f"Saved {df_out.count()} price changes")

# Execute
spark = SparkSession.builder.appName("RetailPricingEngine").getOrCreate()
pricing = LoadPricingData("electronics", "all")
optimized = OptimizePrices(pricing, spark.table("competitors.prices"), spark.table("forecast.demand"), spark.table("supply.inventory"), spark.table("ref.seasons"), spark.table("mktg.promos"), spark.table("finance.costs"), spark.table("finance.margin_targets"), spark.table("analytics.cannibalization"), spark.table("mktg.bundles"), spark.table("clearance.schedule"))
ApplyPriceChanges(optimized)
