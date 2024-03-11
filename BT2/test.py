from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, year

# Create SparkSession
spark = SparkSession.builder \
    .appName("Timestamp to Year Conversion") \
    .getOrCreate()

# Sample Unix timestamp
unix_timestamp = 964981247

# Convert Unix timestamp to year
converted_year = spark.createDataFrame([(unix_timestamp,)], ["timestamp"])

converted_year = converted_year.withColumn("year", year(from_unixtime("timestamp")))
converted_year.show(truncate=False)