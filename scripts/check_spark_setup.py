# Quick smoke test to confirm the local PySpark/Java setup works
# before running the actual pipeline.
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("test").getOrCreate()
spark.range(5).show()
spark.stop()
