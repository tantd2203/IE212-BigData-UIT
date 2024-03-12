from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("Cau1").getOrCreate()

# Read movie and rating data
movieDF = spark.read.options(delimiter=',').schema('movie_id INT, title STRING, genres STRING').csv("data/movies_small.csv")
ratingDF = spark.read.options(delimiter=',').schema('user_id INT, movie_id INT, rating DOUBLE, timestamp STRING').csv("data/ratings_small.csv")


# Cau 1: Tính số lượng phim theo năm
temp1 = movieDF.withColumn("Year",
                           F.when(F.regexp_extract("title", r"\((\d{4}(\–\d{4})?)\)", 1) != "",
                                  F.regexp_extract("title", r"\((\d{4}(\–\d{4})?)\)", 1).cast("int"))
                            .otherwise(F.lit(' '))
                          )
temp1 = temp1.na.drop(subset=["Year"])
result1 = temp1.groupBy("Year").agg(F.count("year").alias("Num_of_movies"))
result1 = result1.sort(result1['Year'].cast('int'))
result1.show(100, truncate=False)
