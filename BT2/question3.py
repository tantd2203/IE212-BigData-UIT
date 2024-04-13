
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import avg, explode, split

spark = SparkSession.builder.appName("cau3").getOrCreate()

# Read movie and rating data
movieDF = spark.read.options(delimiter=',').schema('movie_id INT, title STRING, genres STRING').csv("data/movies_small.csv")
ratingDF = spark.read.options(delimiter=',').schema('user_id INT, movie_id INT, rating DOUBLE, timestamp STRING').csv("data/ratings_small.csv")


# Join và tính trung bình rating theo genres và userId
joinCau3 = ratingDF.join(movieDF, on=['movie_id'], how="inner")
cau3 = joinCau3.withColumn("Genre", explode(split("genres", "\|")))
result_cau3 = cau3.groupBy("user_id", "Genre").agg(avg("rating").alias("avg_rating"))
result_cau3 = result_cau3.groupBy("user_id").pivot("Genre").avg("avg_rating")
# Sắp xếp theo userId và hiển thị 100 dòng mà không truncate
sorted_cau3 = result_cau3.orderBy("user_id")
sorted_cau3.show(10, truncate=False)

# join3 = ratingDF.join(movieDF, on=['movieId'], how="inner")
# join3 = join3.withColumn("Genre", explode(split("genres", "\|")))
# join3 = join3.groupBy("userId", "Genre").agg(avg("rating").alias("avg_rating"))
# result3 = join3.groupBy("userId").pivot("Genre").agg(avg("avg_rating").alias("avg_rating"))
# result3 = result3.orderBy("userId")
# result3.show(100, truncate=False)