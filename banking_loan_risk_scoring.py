# Databricks notebook source
# MAGIC %md
# MAGIC # Banking — Loan Risk Scoring & Credit Assessment
# MAGIC ML-based credit risk model for loan approvals.

# COMMAND ----------

# Cell 1: Imports
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
import pickle
import os
from datetime import *


# COMMAND ----------

# Cell 2: Credentials & Config — HARDCODED (BLOCKER)
ORACLE_CONN = "jdbc:oracle:thin:admin/B@nking2026!@loan-db-prod.bank.internal:1521/LOANPROD"
AWS_ACCESS_KEY = "AKIAIOSFODNN7EXAMPLE"
AWS_SECRET_KEY = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

# Hardcoded path
MODEL_PATH = "/dbfs/mnt/models/credit_risk/v2.1/model.pkl"
DATA_PATH = "s3://bank-data-prod/loans/2026/"


# COMMAND ----------

# Cell 3: Customer Data Load — PII, SQL Injection
def loadCustomerData(loan_type, region):
    # SQL injection via string concat
    query = "SELECT customer_id, first_name, last_name, ssn, annual_income, credit_score, " + \
            "employment_status, home_address, bank_account_number, " + \
            "loan_amount, loan_term, interest_rate " + \
            "FROM customers WHERE loan_type = '" + loan_type + "' AND region = '" + region + "'"
    
    df = spark.read.format("jdbc").option("url", ORACLE_CONN).option("query", query).load()
    
    # Logging PII
    print(f"Customer sample: SSN={df.first().ssn}, Income={df.first().annual_income}")
    
    return df


# COMMAND ----------

# Cell 4: Feature Engineering — Magic Numbers, Complexity
def BuildFeatures(df):
    # Magic numbers everywhere
    df = df.withColumn("dti_ratio", col("loan_amount") / col("annual_income"))
    df = df.withColumn("risk_bucket",
        when(col("credit_score") >= 750, 0)
        .when(col("credit_score") >= 700, 1)
        .when(col("credit_score") >= 650, 2)
        .when(col("credit_score") >= 600, 3)
        .otherwise(4))
    
    df = df.withColumn("income_bracket",
        when(col("annual_income") > 200000, "high")
        .when(col("annual_income") > 80000, "medium")
        .otherwise("low"))
    
    df = df.withColumn("ltv_ratio", col("loan_amount") / (col("property_value") * 0.95))
    df = df.withColumn("monthly_payment", (col("loan_amount") * (col("interest_rate")/12/100) * (1 + col("interest_rate")/12/100)**360) / ((1 + col("interest_rate")/12/100)**360 - 1))
    
    # Bare except
    try:
        df = df.withColumn("zip_risk", col("zip_code").substr(1, 3).cast("int") % 5)
    except:
        pass
    
    return df


# COMMAND ----------

# Cell 5: Model Training — Dead Code, Duplication
def TrainModel(df):
    # Dead imports
    import tensorflow as tf
    import matplotlib.pyplot as plt
    
    feature_cols = ["credit_score", "annual_income", "dti_ratio", "risk_bucket", "loan_amount", "loan_term"]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    df_vec = assembler.transform(df)
    
    train, test = df_vec.randomSplit([0.8, 0.2], seed=42)
    
    rf = RandomForestClassifier(featuresCol="features", labelCol="default_flag", numTrees=100, maxDepth=10)
    model = rf.fit(train)
    
    # Duplicate evaluation block
    predictions = model.transform(test)
    accuracy = predictions.filter(col("prediction") == col("default_flag")).count() / predictions.count()
    print(f"Model accuracy: {accuracy}")
    
    predictions = model.transform(test)
    accuracy = predictions.filter(col("prediction") == col("default_flag")).count() / predictions.count()
    print(f"Model accuracy: {accuracy}")
    
    # Unused variable
    old_model = None
    deprecated_threshold = 0.65
    
    return model


# COMMAND ----------

# Cell 6: Batch Scoring — collect() on large data
def ScoreLoanApplications(model, new_apps_df):
    scored = model.transform(new_apps_df)
    
    # Collecting entire dataset into driver
    all_scores = scored.select("customer_id", "prediction", "probability").collect()
    
    results = []
    for row in all_scores:
        results.append({
            "customer_id": row.customer_id,
            "approved": row.prediction == 0,
            "risk_prob": float(row.probability[1])
        })
    
    return results

# Run pipeline
data = loadCustomerData("mortgage", "northeast")
features = BuildFeatures(data)
model = TrainModel(features)
scores = ScoreLoanApplications(model, features)
print(f"Scored {len(scores)} applications")
