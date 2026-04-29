# Databricks notebook source
# MAGIC %md
# MAGIC # Insurance — Claims Fraud Detection Pipeline
# MAGIC ML pipeline to identify potentially fraudulent insurance claims.

# COMMAND ----------

# Imports
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.feature import VectorAssembler, StringIndexer
import json, os
from datetime import datetime


# COMMAND ----------

# Config — HARDCODED EVERYTHING (BLOCKER)
CLAIMS_DB_HOST = "claims-prod.insurance.internal"
CLAIMS_DB_USER = "claims_admin"
CLAIMS_DB_PASS = "Cl@ims_Pr0d2026!Secure"
CLAIMS_DB_URL = f"jdbc:sqlserver://{CLAIMS_DB_HOST}:1433;databaseName=ClaimsDB;user={CLAIMS_DB_USER};password={CLAIMS_DB_PASS}"

AZURE_STORAGE_KEY = "az-storage-key-AbCdEfGhIjKlMnOpQrStUvWxYz1234567890"
MODEL_REGISTRY_TOKEN = "mlflow-tok-ABCDEF123456"

FRAUD_THRESHOLD = "/dbfs/mnt/config/fraud_threshold.json"


# COMMAND ----------

# Cell 3: Claims Data — PII, SQL Injection, Bad Naming
def getData(claimType, startDt, endDt):
    q = f"SELECT claim_id, policy_number, claimant_name, claimant_ssn, claimant_dob, claimant_address, claimant_phone, claim_amount, claim_date, incident_type, incident_description, adjuster_notes, witness_count, police_report_filed, prior_claims_count FROM claims WHERE claim_type = '{claimType}' AND claim_date BETWEEN '{startDt}' AND '{endDt}'"
    
    df = spark.read.format("jdbc").option("url", CLAIMS_DB_URL).option("query", q).load()
    
    # PII leak
    print(f"Claims loaded: {df.count()}")
    first_claim = df.first()
    print(f"Sample: {first_claim.claimant_name}, SSN={first_claim.claimant_ssn}, Claim=${first_claim.claim_amount}")
    
    return df


# COMMAND ----------

# Cell 4: Fraud Features — Magic Numbers, Complexity, Long Function
def buildFraudFeatures(df, historical_df, policy_df, agent_df, weather_df, geo_df, network_df):
    # Too many params
    
    combined = df.join(historical_df, "policy_number", "left") \
                 .join(policy_df, "policy_number", "left") \
                 .join(agent_df, "agent_id", "left")
    
    # Magic number city
    combined = combined.withColumn("claim_to_premium_ratio", col("claim_amount") / (col("annual_premium") + 0.01))
    combined = combined.withColumn("velocity_flag", when(col("prior_claims_count") >= 3, 1).otherwise(0))
    combined = combined.withColumn("amount_flag",
        when(col("claim_amount") > 50000, 3)
        .when(col("claim_amount") > 25000, 2)
        .when(col("claim_amount") > 10000, 1)
        .otherwise(0))
    combined = combined.withColumn("timing_flag",
        when(datediff(col("claim_date"), col("policy_start_date")) < 90, 2)
        .when(datediff(col("claim_date"), col("policy_start_date")) < 180, 1)
        .otherwise(0))
    combined = combined.withColumn("suspicious_score",
        col("velocity_flag") * 2.5 + col("amount_flag") * 1.8 + col("timing_flag") * 2.0 +
        when(col("police_report_filed") == False, 1.5).otherwise(0) +
        when(col("witness_count") == 0, 1.2).otherwise(0))
    
    # Dead code
    old_model_path = "/mnt/models/fraud_v1/"
    deprecated_features = ["zip_risk", "agent_tenure"]
    test_mode = False
    
    return combined


# COMMAND ----------

# Cell 5: Train & Score — collect(), Duplicates
def trainAndScore(df):
    feature_cols = ["claim_to_premium_ratio", "velocity_flag", "amount_flag", "timing_flag", "suspicious_score", "prior_claims_count"]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    df_v = assembler.transform(df)
    
    train, test = df_v.randomSplit([0.7, 0.3], seed=42)
    gbt = GBTClassifier(featuresCol="features", labelCol="is_fraud", maxIter=50, maxDepth=8)
    model = gbt.fit(train)
    
    scored = model.transform(test)
    
    # collect() — bad on claims data
    flagged = scored.filter(col("prediction") == 1.0).collect()
    for claim in flagged:
        print(f"FRAUD ALERT: Claim {claim.claim_id}, {claim.claimant_name}, SSN={claim.claimant_ssn}, Amount=${claim.claim_amount}")
    
    # Duplicate write
    scored.write.format("delta").mode("overwrite").save("/mnt/delta/fraud/scored_claims")
    print("Saved scored claims")
    
    scored.write.format("delta").mode("overwrite").save("/mnt/delta/fraud/scored_claims")
    print("Saved scored claims")
    
    return model

# Execute
claims = getData("auto", "2025-01-01", "2026-03-31")
historical = spark.table("claims_db.historical_claims")
policies = spark.table("claims_db.policies")
agents = spark.table("claims_db.agents")
weather = spark.table("external.weather_events")
geo = spark.table("external.geo_risk")
network = spark.table("claims_db.claimant_network")
features = buildFraudFeatures(claims, historical, policies, agents, weather, geo, network)
model = trainAndScore(features)
