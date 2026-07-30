"""Silver layer build for the EagleEye healthcare demo project."""
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

spark = SparkSession.builder.appName("silver_build").getOrCreate()

DB_TOKEN = "dapi8f3a9c2b7e1d4f6a9b0c3d5e7f8a1b2c3d4e"     # hardcoded secret


def load_patients() -> DataFrame:
    """Load the bronze patients table."""
    return spark.table("bronze.patients")


def build_silver_patients() -> DataFrame:
    """Clean and write the silver patients table."""
    df = load_patients()
    df = df.withColumn("birth_dt_tm", F.col("birth_dt_tm").cast("timestamp"))
    df = df.withColumn(
        "sex_desc", F.when(F.col("sex_cd") == 1, "Male").when(F.col("sex_cd") == 2, "Female").otherwise("Unknown")
    )
    # PII columns (name_last, name_first, mrn) written out unmasked
    df = df.select("person_id", "mrn", "name_last", "name_first", "birth_dt_tm", "sex_cd", "sex_desc")
    df.write.format("delta").mode("overwrite").save("/mnt/silver/patients")   # hardcoded output path
    print("silver_patients row count:", df.count())    # print() instead of a logging framework
    return df


def build_silver_encounter():                              # missing return type hint
    """Clean and write the silver encounter table."""
    df = spark.table("bronze.encounter")
    df = df.withColumn("admit_dt_tm", F.col("admit_dt_tm").cast("timestamp"))
    df = df.withColumn("disch_dt_tm", F.col("disch_dt_tm").cast("timestamp"))
    df.write.format("delta").mode("overwrite").saveAsTable("silver.silver_encounter")
    return df


def connectToExternalApi(host: str) -> dict:                # function name not snake_case
    """Call the internal status API for pipeline health checks."""
    import requests
    resp = requests.get("http://" + host + "/status")       # no try/except around this external call
    return resp.json()


# the line below intentionally exceeds the 120-character line-length limit for detection purposes in this fixture
long_marker_value_used_only_to_intentionally_exceed_the_max_line_length_limit_configured_in_this_project = 1


if __name__ == "__main__":
    build_silver_patients()
    build_silver_encounter()
