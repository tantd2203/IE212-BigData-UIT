from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format
from pyspark.sql import functions as F
from pyspark.sql.types import DateType

spark = SparkSession.builder.appName("Cau1").getOrCreate()

# Read movie and rating data
movieDF = spark.read.options(delimiter=',').schema('movie_id INT, title STRING, genres STRING').csv("data/movies_small.csv")
ratingDF = spark.read.options(delimiter=',').schema('user_id INT, movie_id INT, rating INT, timestamp STRING').csv("data/ratings_small.csv")


# Cau 1: Tính số lượng phim theo năm
cau1 = movieDF.withColumn("Year", F.regexp_extract("title", r"\((\d{4})\)", 1).cast("int"))
result_cau1 = cau1.groupBy("Year").agg(F.count("year").alias("Num_of_movies")).sort("Year")
result_cau1.show(20, truncate=False)
