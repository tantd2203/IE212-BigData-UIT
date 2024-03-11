from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format
from pyspark.sql import functions as F
from pyspark.sql.types import DateType

# Create SparkSession
spark = SparkSession.builder.appName("TimestampConversion").getOrCreate()

# Read movie and rating data
movieDF = spark.read.options(delimiter=',').schema('movie_id INT, title STRING, genres STRING').csv("data/movies_small.csv")
ratingDF = spark.read.options(delimiter=',').schema('user_id INT, movie_id INT, rating INT, timestamp STRING').csv("data/ratings_small.csv")


result  =ratingDF.withColumn('year',F.from_unixtime('timestamp').cast(DateType())).withColumn('year',date_format('year','yyyy'))

