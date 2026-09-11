from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

spark = SparkSession.builder.appName("jobs").getOrCreate()

CITIES = ["Brno", "Vienna", "Remote"]

df = None
for city in CITIES:
    part = (spark.read.option("multiLine", True)
                 .json(f"data/raw/{city}_jobs.json")
                 .withColumn("city", lit(city)))
    df = part if df is None else df.unionByName(part, allowMissingColumns=True)

print("total rows:", df.count())
df.printSchema()
df.show(5, truncate=60)
