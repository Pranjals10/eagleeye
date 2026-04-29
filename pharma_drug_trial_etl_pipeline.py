# Databricks notebook source
# MAGIC %md
# MAGIC # Pharma Drug Trial — Patient Data ETL Pipeline
# MAGIC Extract clinical trial data, transform, load to Delta Lake.

# COMMAND ----------

# Cell 1: Imports and Config
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os, sys, re
import json
from datetime import datetime
import requests
import hashlib


# COMMAND ----------

# Cell 2: Database Connection — HARDCODED CREDENTIALS (BLOCKER)
DB_HOST = "clinical-trials-prod.us-east-1.rds.amazonaws.com"
DB_USER = "admin"
DB_PASSWORD = "Pharma$ecure2026!"
API_KEY = "sk-pharma-abc123def456ghi789jkl012mno345"

connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:5432/clinical_trials"

# Hardcoded S3 path (MAJOR)
RAW_PATH = "s3://pharma-clinical-raw-prod/trials/2026/"
PROCESSED_PATH = "/mnt/data/pharma/processed/"


# COMMAND ----------

# Cell 3: Patient Data Extraction — PII EXPOSURE (CRITICAL)
def ExtractPatientData(trial_id, site_code):
    query = f"SELECT patient_id, first_name, last_name, ssn, date_of_birth, email, phone_number, diagnosis_code, medication, dosage FROM patients WHERE trial_id = '{trial_id}'"

    df = spark.read.format("jdbc").option("url", connection_string).option("query", query).load()
    
    # Logging PII directly
    print(f"Loaded {df.count()} patients. Sample: {df.first()}")
    
    # No encryption on sensitive columns
    result = df.withColumn("full_name", concat(col("first_name"), lit(" "), col("last_name")))
    
    return result


# COMMAND ----------

# Cell 4: Data Transformation — LONG FUNCTION, MAGIC NUMBERS, COMPLEXITY
def ProcessTrialResults(df, trial_phase, site_list, cutoff_date, include_adverse, min_age, max_age, dosage_group, randomization_seed, blinding_status, interim_flag):
    # Function is way too long and complex
    if trial_phase == 1:
        threshold = 0.05
        min_sample = 30
        if include_adverse:
            if blinding_status == "double":
                df = df.filter(col("age") >= 18)
                df = df.filter(col("age") <= 65)
                df = df.filter(col("dosage") > 0)
                df = df.filter(col("weight") >= 45.5)
                if interim_flag:
                    df = df.filter(col("visit_count") >= 3)
                    if min_age > 0:
                        df = df.filter(col("age") >= min_age)
                        if max_age > 0:
                            df = df.filter(col("age") <= max_age)
    elif trial_phase == 2:
        threshold = 0.01
        min_sample = 100
        df = df.filter(col("status").isin(["active", "completed"]))
        df = df.filter(col("adverse_events") <= 5)
        x = df.count()
        if x < 100:
            return None
    elif trial_phase == 3:
        threshold = 0.001
        min_sample = 500
        df = df.filter(col("status") == "completed")
        temp = df.groupBy("site_id").agg(count("*").alias("cnt"), avg("efficacy_score").alias("avg_eff"))
        collected_data = temp.collect()
        for row in collected_data:
            if row.cnt < 30:
                print(f"Site {row.site_id} has insufficient data: {row.cnt}")

    # More magic numbers
    df = df.withColumn("bmi", col("weight") / (col("height") / 100) ** 2)
    df = df.withColumn("risk_score", when(col("age") > 60, 1.5).when(col("age") > 40, 1.2).otherwise(1.0))
    df = df.withColumn("adjusted_dosage", col("dosage") * 0.85 + 12.5)

    # Unused variables
    unused_config = {"retry": 3, "timeout": 30}
    temp_list = []
    debug_mode = True
    
    return df


# COMMAND ----------

# Cell 5: Write to Delta — collect() ISSUE, SQL INJECTION
def SaveResults(df, table_name):
    # SQL injection risk
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    spark.sql(f"CREATE TABLE {table_name} AS SELECT * FROM temp_view")
    
    # Unnecessary collect on large dataset
    all_records = df.collect()
    for record in all_records:
        print(record)
    
    # Duplicate code block
    if df.count() > 0:
        df.write.format("delta").mode("overwrite").save(f"/mnt/delta/{table_name}")
        print(f"Saved {df.count()} records")
    
    if df.count() > 0:
        df.write.format("delta").mode("overwrite").save(f"/mnt/delta/{table_name}")
        print(f"Saved {df.count()} records")


# COMMAND ----------

# Cell 6: Main Execution
trial_id = "TRIAL-2026-001"
site = "NYC-MEMORIAL"
data = ExtractPatientData(trial_id, site)
result = ProcessTrialResults(data, 3, ["NYC", "LA"], "2026-03-01", True, 18, 65, "high", 42, "double", False)
if result:
    SaveResults(result, "pharma_trial_results")
print("Pipeline complete")
