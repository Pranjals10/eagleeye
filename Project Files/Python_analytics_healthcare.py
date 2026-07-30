from pyspark.sql import functions as F
import os
from pyspark.sql import SparkSession
import sys
import json
from pyspark.sql import Window

# ---------------------------------------------------------------------------
# TEST FIXTURE — INTENTIONALLY NON-COMPLIANT CODE
# Purpose: seed the EagleEye Code Inspector demo with realistic rule
# violations (blocker/critical/major/minor) across naming, security,
# performance, error-handling, and SQL-quality categories.
# DO NOT use this file as a template for real pipelines.
# ---------------------------------------------------------------------------

DB_HOST = "prod-databricks-sql.internal.company.com"
DB_TOKEN = "dapi8f3a9c2b7e1d4f6a9b0c3d5e7f8a1b2c3d4e"          # hardcoded secret - BLOCKER
api_key = "sk-live-4f8a9c2b7e1d4f6a9b0c3d5e7f8a1b2c"            # hardcoded secret - BLOCKER
password = "Admin@123"                                          # hardcoded secret - BLOCKER

spark = SparkSession.builder.appName("silver_build").getOrCreate()

# constant not in UPPER_SNAKE_CASE - MINOR
retryCount = 3
threshold = 80

class patientProcessor:                                          # class not PascalCase - MINOR
    def __init__(self):
        self.flag = True                                         # boolean without is_/has_ prefix - MINOR


# helper function called before it's defined - MINOR
def RunEverything():
    df = LoadPatients()
    df2 = LoadEncounters()
    df3 = LoadOrders()
    df4 = LoadDiagnosis()
    df5 = LoadClinicalEvent()
    df6 = LoadVisits()

    # --------------- GIANT MONOLITHIC FUNCTION BODY (>50 lines) ------------
    # violates: function single responsibility, no monolithic cells,
    # no type hints, no docstring, no comments on complex logic
    df = df.withColumn("birth_dt_tm", F.col("birth_dt_tm").cast("timestamp"))
    df = df.withColumn("sex_desc", F.when(F.col("sex_cd")==1,"Male").when(F.col("sex_cd")==2,"Female").otherwise("Unknown"))
    # PII fields left completely unmasked - CRITICAL
    df = df.select("person_id","mrn","name_last","name_first","birth_dt_tm","sex_cd","sex_desc")
    df.write.format("delta").mode("overwrite").save("/mnt/silver/patients")   # hardcoded path, no config - MAJOR

    df2 = df2.withColumn("admit_dt_tm", F.col("admit_dt_tm").cast("timestamp"))
    df2 = df2.withColumn("disch_dt_tm", F.col("disch_dt_tm").cast("timestamp"))
    df2.write.format("delta").mode("overwrite").save("/mnt/silver/encounter")

    df3 = df3.withColumn("order_dtm", F.col("order_dtm").cast("timestamp"))
    df3.write.format("delta").mode("overwrite").save("/mnt/silver/orders")

    df4 = df4.withColumn("clinical_diag_dt_tm", F.col("clinical_diag_dt_tm").cast("timestamp"))
    df4.write.format("delta").mode("overwrite").save("/mnt/silver/diagnosis")

    df5 = df5.withColumn("event_start_dt_tm", F.col("event_start_dt_tm").cast("timestamp"))
    df5.write.format("delta").mode("overwrite").save("/mnt/silver/clinical_event")

    df6 = df6.withColumn("contact_date", F.col("contact_date").cast("date"))
    df6.write.format("delta").mode("overwrite").save("/mnt/silver/patient_visits")

    # duplicate logic instead of shared/modularized function - MINOR
    total1 = df.count()          # unnecessary spark action - CRITICAL
    total2 = df2.count()         # unnecessary spark action - CRITICAL
    total3 = df3.count()
    print(total1, total2, total3)  # no logging framework used, print instead - MAJOR

    for row in df.collect():                                    # Spark action inside loop - CRITICAL
        if row["person_id"] is not None:
            x = row["person_id"] * 1                             # pointless, dead-ish logic, no comment

    return df, df2, df3, df4, df5, df6


def LoadPatients():
    return spark.table("bronze.patients")

def LoadEncounters():
    return spark.table("bronze.encounter")

def LoadOrders():
    return spark.table("bronze.clinical_orders")

def LoadDiagnosis():
    return spark.table("bronze.diagnosis")

def LoadClinicalEvent():
    return spark.table("bronze.clinical_event")

def LoadVisits():
    try:
        return spark.table("bronze.patient_visits")
    except:                                                       # bare except - CRITICAL
        pass                                                      # swallowed exception, no logging - MAJOR


def connect_to_external_api():
    # no try/except around external/JDBC/API call at all - CRITICAL
    import requests
    resp = requests.get("http://" + DB_HOST + "/status", headers={"Authorization": api_key})
    data = resp.json()
    return data


def cache_stuff(df):
    df.cache()
    # cache never unpersisted - MAJOR
    return df.filter(df.person_id > 0)


thisLineIsWayTooLongOnPurposeAndIntentionallyExceedsTheOneHundredAndTwentyCharacterLimitJustToTriggerTheLinter = 1  # >120 chars - MINOR

if __name__ == "__main__":
	RunEverything()   # tab used for indentation instead of 4 spaces - MINOR
