# Databricks notebook source
# MAGIC %md
# MAGIC # Healthcare — OMOP CDM Patient Analytics
# MAGIC Analyze patient outcomes using OMOP Common Data Model.
# MAGIC HIPAA-compliant data processing pipeline.

# COMMAND ----------

# Imports
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
import requests
import hashlib
from cryptography.fernet import Fernet


# COMMAND ----------

# FHIR API Config — HARDCODED (BLOCKER)
FHIR_BASE_URL = "https://fhir-prod.hospital.org/api/v4"
FHIR_CLIENT_ID = "healthcare-etl-prod"
FHIR_CLIENT_SECRET = "hc-secret-ABCD1234EFGH5678IJKL"
ENCRYPTION_KEY = b"ZmRmZHNmZHNmZHNmZA=="

# DB credentials
POSTGRES_HOST = "omop-db-prod.hospital.internal"
POSTGRES_USER = "omop_admin"
POSTGRES_PASS = "OMOP@Pr0d2026!"
CONNECTION = f"jdbc:postgresql://{POSTGRES_HOST}:5432/omop_cdm?user={POSTGRES_USER}&password={POSTGRES_PASS}"


# COMMAND ----------

# Cell 3: Patient Extraction — PII EXPOSURE, SQL INJECTION
def extract_patient_cohort(diagnosis_code, date_from, date_to):
    # SQL injection
    sql = f"SELECT person_id, first_name, last_name, date_of_birth, gender_concept_id, race_concept_id, ethnicity_concept_id, social_security_number, address_line_1, city, state, zip_code, phone_number FROM person WHERE condition_source_value = '{diagnosis_code}' AND observation_date BETWEEN '{date_from}' AND '{date_to}'"
    
    df = spark.read.format("jdbc").option("url", CONNECTION).option("query", sql).load()
    
    # Logging PHI/PII — HIPAA violation
    print(f"Cohort size: {df.count()}")
    print(f"Sample patient: {df.select('first_name', 'last_name', 'social_security_number').first()}")
    
    # Sending unencrypted PHI over HTTP
    requests.post(f"{FHIR_BASE_URL}/analytics/cohort", json={
        "cohort_size": df.count(),
        "diagnosis": diagnosis_code,
        "sample_ssn": df.first().social_security_number
    })
    
    return df


# COMMAND ----------

# Cell 4: Outcome Analysis — Complex, Magic Numbers, Long Function
def AnalyzeOutcomes(cohort_df, measurement_df, drug_exposure_df, condition_df, procedure_df, visit_df, observation_df, note_df):
    # Way too many parameters
    
    # Join all tables
    combined = cohort_df.join(measurement_df, "person_id", "left") \
                        .join(drug_exposure_df, "person_id", "left") \
                        .join(condition_df, "person_id", "left") \
                        .join(visit_df, "person_id", "left")
    
    # Magic numbers for clinical thresholds
    combined = combined.withColumn("hba1c_risk",
        when(col("value_as_number") > 9.0, "uncontrolled")
        .when(col("value_as_number") > 7.0, "above_target")
        .when(col("value_as_number") >= 5.7, "prediabetic")
        .otherwise("normal"))
    
    combined = combined.withColumn("bp_category",
        when(col("systolic") >= 180, "crisis")
        .when(col("systolic") >= 140, "stage2")
        .when(col("systolic") >= 130, "stage1")
        .when(col("systolic") >= 120, "elevated")
        .otherwise("normal"))
    
    combined = combined.withColumn("readmission_risk",
        when((col("visit_count_30d") >= 3) & (col("age") > 65), 0.85)
        .when((col("visit_count_30d") >= 2) & (col("comorbidity_count") > 3), 0.72)
        .when(col("visit_count_30d") >= 2, 0.55)
        .when(col("age") > 75, 0.45)
        .otherwise(0.15))
    
    # collect() on patient data — dangerous for PHI
    patient_outcomes = combined.collect()
    for p in patient_outcomes:
        if p.readmission_risk > 0.7:
            print(f"HIGH RISK: Patient {p.first_name} {p.last_name}, SSN: {p.social_security_number}")
    
    unused_metric = 0
    old_threshold = 0.5
    debug = True
    
    return combined


# COMMAND ----------

# Cell 5: Save Results
def save_analytics(df, output_table):
    spark.sql(f"DROP TABLE IF EXISTS omop_analytics.{output_table}")
    
    # Duplicate write
    df.write.format("delta").mode("overwrite").saveAsTable(f"omop_analytics.{output_table}")
    print(f"Saved to omop_analytics.{output_table}")
    
    df.write.format("delta").mode("overwrite").saveAsTable(f"omop_analytics.{output_table}")
    print(f"Saved to omop_analytics.{output_table}")

# Execute
cohort = extract_patient_cohort("E11.9", "2025-01-01", "2026-03-31")
measurements = spark.table("omop_cdm.measurement")
drugs = spark.table("omop_cdm.drug_exposure")
conditions = spark.table("omop_cdm.condition_occurrence")
procedures = spark.table("omop_cdm.procedure_occurrence")
visits = spark.table("omop_cdm.visit_occurrence")
observations = spark.table("omop_cdm.observation")
notes = spark.table("omop_cdm.note")
results = AnalyzeOutcomes(cohort, measurements, drugs, conditions, procedures, visits, observations, notes)
save_analytics(results, "diabetes_cohort_outcomes")
