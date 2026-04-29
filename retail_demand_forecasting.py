# Databricks notebook source
# MAGIC %md
# MAGIC # Retail — Demand Forecasting & Inventory Optimization
# MAGIC Predict demand across stores and optimize stock levels.

# COMMAND ----------

# Imports
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.feature import VectorAssembler
import numpy as np
import json


# COMMAND ----------

# Config — HARDCODED SECRETS
REDSHIFT_URL = "jdbc:redshift://retail-cluster-prod.abc123.us-west-2.redshift.amazonaws.com:5439/retaildb"
REDSHIFT_USER = "etl_service"
REDSHIFT_PASS = "R3tail_Pr0d#2026"

WEATHER_API_KEY = "wx-api-key-1234567890abcdef"
INVENTORY_API_TOKEN = "inv-tok-ABCDEFGHIJKLMNOP123456"


# COMMAND ----------

# Cell 3: Sales Data Load — SQL Injection, PII
def Load_Sales_Data(store_id, category):
    q = f"SELECT transaction_id, customer_email, loyalty_card_number, product_sku, quantity, unit_price, discount_pct, store_id, transaction_date, payment_method, credit_card_last4 FROM sales WHERE store_id = '{store_id}' AND category = '{category}'"
    
    df = spark.read.format("jdbc").option("url", REDSHIFT_URL).option("user", REDSHIFT_USER).option("password", REDSHIFT_PASS).option("query", q).load()
    
    print(f"Loaded {df.count()} transactions. Customer email sample: {df.first().customer_email}")
    return df


# COMMAND ----------

# Cell 4: Feature Engineering — Magic Numbers, Vague Names
def process(df):
    x = df.withColumn("dow", dayofweek(col("transaction_date")))
    x = x.withColumn("m", month(col("transaction_date")))
    x = x.withColumn("wk", weekofyear(col("transaction_date")))
    
    # Magic numbers
    x = x.withColumn("promo_flag", when(col("discount_pct") > 15.0, 1).otherwise(0))
    x = x.withColumn("high_value", when(col("unit_price") * col("quantity") > 500, 1).otherwise(0))
    x = x.withColumn("seasonal_factor",
        when(col("m").isin([11, 12]), 1.45)
        .when(col("m").isin([6, 7, 8]), 1.15)
        .when(col("m").isin([1, 2]), 0.75)
        .otherwise(1.0))
    
    x = x.withColumn("loyalty_tier",
        when(col("purchase_frequency") >= 20, "platinum")
        .when(col("purchase_frequency") >= 10, "gold")
        .when(col("purchase_frequency") >= 5, "silver")
        .otherwise("bronze"))
    
    # Unused
    tmp = []
    old_calc = None
    
    return x


# COMMAND ----------

# Cell 5: Forecast — Duplicate Code, collect()
def forecast_demand(df, horizon):
    feature_cols = ["dow", "m", "wk", "promo_flag", "seasonal_factor"]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    df_feat = assembler.transform(df)
    
    train, test = df_feat.randomSplit([0.8, 0.2], seed=42)
    gbt = GBTRegressor(featuresCol="features", labelCol="quantity", maxIter=100)
    model = gbt.fit(train)
    
    preds = model.transform(test)
    
    # Collecting all predictions
    all_preds = preds.collect()
    for p in all_preds:
        if p.prediction > 100:
            print(f"High demand alert: SKU={p.product_sku}, predicted={p.prediction}")
    
    # Duplicate save block
    preds.write.format("delta").mode("overwrite").save("/mnt/delta/retail/forecasts")
    print("Saved forecasts")
    
    preds.write.format("delta").mode("overwrite").save("/mnt/delta/retail/forecasts")
    print("Saved forecasts")
    
    return model

# Run
sales = Load_Sales_Data("STORE-101", "electronics")
features = process(sales)
model = forecast_demand(features, 30)
print("Demand forecasting complete")
