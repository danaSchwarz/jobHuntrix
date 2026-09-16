from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, when, trim, regexp_replace

spark = SparkSession.builder.appName("jobs").getOrCreate()


CITIES = ["Brno", "Vienna", "Remote"]

df = None

for city in CITIES:
    part = (spark.read.option("multiLine", True)
                 .json(f"data/raw/{city}_jobs.json")
                 .withColumn("city", lit(city)))
    df = part if df is None else df.unionByName(part, allowMissingColumns=True)

# print("total rows:", df.count())
# df.printSchema()
# df.show(5, truncate=60)

df = df.dropDuplicates(["id"])

df = df.withColumn("salary", when(trim(col("salary"))
                   == "", None).otherwise(col("salary")))

# print("rows after dedupe:", df.count())
# df.select("title", "city", "company", "salary").show(10, truncate=40)

df = df.withColumn(
    "currency",
    when(col("salary").contains("Kč"), "CZK")
    .when(col("salary").contains("€"), "EUR")
    .when(col("salary").contains("$"), "USD")
    .otherwise(None)
)

df = df.withColumn(
    "salary_num",
    regexp_replace(col("salary"), "[^0-9]", "").cast("double")
)

df.select("city", "salary", "currency", "salary_num").show(15, truncate=30)
