# Databricks notebook source
# MAGIC %md
# MAGIC # Energy — Smart Grid Anomaly Detection
# MAGIC Detect anomalies in power grid sensor data for predictive maintenance.

# COMMAND ----------

# Imports
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
import json


# COMMAND ----------

# Config — HARDCODED
INFLUXDB_URL = "https://grid-metrics-prod.energy.internal:8086"
INFLUXDB_TOKEN = "influx-token-ABCDEFGHIJKLMNOPQRSTUVWXYZ123456"
INFLUXDB_ORG = "energy-corp"

SCADA_API_KEY = "scada-api-key-987654321fedcba"
ALERT_WEBHOOK = "https://hooks.slack.com/services/T00/B00/XXXXX"


# COMMAND ----------

# Cell 3: Sensor Data Ingest — PII-light but has hardcoded paths
def ingest_sensor_data(grid_zone, sensor_type):
    raw_path = f"/mnt/raw/grid/{grid_zone}/{sensor_type}/"
    
    schema = StructType([
        StructField("sensor_id", StringType()),
        StructField("timestamp", TimestampType()),
        StructField("voltage", DoubleType()),
        StructField("current", DoubleType()),
        StructField("frequency", DoubleType()),
        StructField("temperature", DoubleType()),
        StructField("power_factor", DoubleType()),
        StructField("operator_name", StringType()),
        StructField("operator_badge_id", StringType()),
    ])
    
    df = spark.read.format("json").schema(schema).load(raw_path)
    
    # Logging operator info
    print(f"Ingested {df.count()} readings from zone {grid_zone}")
    print(f"Operators: {[r.operator_name for r in df.select('operator_name').distinct().collect()]}")
    
    return df


# COMMAND ----------

# Cell 4: Anomaly Features — Magic numbers, moderate complexity
def compute_anomaly_features(df):
    # Magic number thresholds
    df = df.withColumn("voltage_deviation", abs(col("voltage") - 240.0) / 240.0)
    df = df.withColumn("freq_deviation", abs(col("frequency") - 50.0) / 50.0)
    df = df.withColumn("temp_flag", when(col("temperature") > 85.0, 1).otherwise(0))
    df = df.withColumn("pf_flag", when(col("power_factor") < 0.85, 1).otherwise(0))
    df = df.withColumn("overload_flag", when(col("current") > 400, 1).otherwise(0))
    
    df = df.withColumn("risk_index",
        col("voltage_deviation") * 3.0 +
        col("freq_deviation") * 4.0 +
        col("temp_flag") * 2.0 +
        col("pf_flag") * 1.5 +
        col("overload_flag") * 5.0)
    
    # Unused
    old_weights = [1.0, 1.0, 1.0, 1.0, 1.0]
    calibration = None
    
    return df


# COMMAND ----------

# Cell 5: Clustering & Alert — collect() 
def detect_anomalies(df, n_clusters):
    feature_cols = ["voltage_deviation", "freq_deviation", "temp_flag", "pf_flag", "overload_flag", "risk_index"]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    df_feat = assembler.transform(df)
    
    kmeans = KMeans(k=n_clusters, seed=42, featuresCol="features")
    model = kmeans.fit(df_feat)
    clustered = model.transform(df_feat)
    
    # collect() on sensor data
    anomalies = clustered.filter(col("risk_index") > 8.0).collect()
    for a in anomalies:
        print(f"ANOMALY: Sensor {a.sensor_id}, risk={a.risk_index}, zone operator: {a.operator_name}")
    
    # Duplicate save
    clustered.write.format("delta").mode("overwrite").save("/mnt/delta/grid/anomalies")
    print("Saved anomaly results")
    
    clustered.write.format("delta").mode("overwrite").save("/mnt/delta/grid/anomalies")
    print("Saved anomaly results")
    
    return model

# Run
sensor_data = ingest_sensor_data("ZONE-A", "transformer")
features = compute_anomaly_features(sensor_data)
model = detect_anomalies(features, 5)
