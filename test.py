from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("test").getOrCreate()
spark.range(5).show()
spark.stop()
