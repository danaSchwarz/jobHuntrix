from pyspark.sql.functions import (
    col, when, trim, lit, lower, regexp_extract, regexp_replace, count, avg, round as spark_round
)
from pyspark.sql import SparkSession
import os
os.makedirs("data/clean", exist_ok=True)


spark = SparkSession.builder.appName("jobs").getOrCreate()
# Spark 4 crashes on bad number casts (ANSI mode). For messy real data,
# we want bad values to become null instead of crashing the job:
spark.conf.set("spark.sql.ansi.enabled", "false")

CITIES = ["Brno", "Vienna", "Remote"]

df = None
for city in CITIES:
    part = (spark.read.option("multiLine", True)
                 .json(f"data/raw/{city}_jobs.json")
                 .withColumn("city", lit(city)))
    df = part if df is None else df.unionByName(part, allowMissingColumns=True)

# --- Move 1: dedupe + turn empty salaries into null ---
df = df.dropDuplicates(["id"])
df = df.withColumn("salary",
                   when(trim(col("salary")) == "", None).otherwise(col("salary")))

# --- Move 2: parse salary -> currency + number ---
df = df.withColumn("currency",
                   when(col("salary").contains("Kč"), "CZK")
                   .when(col("salary").contains("€"), "EUR")
                   .when(col("salary").contains("$"), "USD")
                   .otherwise(None))

df = df.withColumn("salary_first",
                   regexp_extract(col("salary"), r"([0-9]+(?:[.,][0-9]+)?)", 1))

df = df.withColumn("has_k", lower(col("salary")).rlike("[0-9]k"))

df = df.withColumn("salary_num",
                   when(col("salary_first") == "", None).otherwise(
                       regexp_replace(col("salary_first"),
                                      ",", ".").cast("double")
                       * when(col("has_k"), 1000).otherwise(1)))

df.select("city", "salary", "currency", "salary_num").show(20, truncate=30)

summary = (df.groupBy("city", "currency")
           .agg(
               count("*").alias("job_count"),
               spark_round(avg("salary_num"), 0).alias("avg_salary")
)
    .orderBy("city", "currency"))

summary.show()

"""
df.write.mode("overwrite").parquet("data'/clean/jobs")
summary.write.mode("overwrite").parquet("data/clean/jobs_summary")
print("done - written to data/clean/")
"""
df.toPandas().to_parquet("data/clean/jobs.parquet", index=False)
summary.toPandas().to_parquet("data/clean/jobs_summary.parquet", index=False)
print("done - written to data/clean/")
