from pyspark.sql import SparkSession
from pyspark.sql import functions as f 
from pyspark.sql.functions import col
spark = SparkSession.builder.appName("Cau1").getOrCreate()

# Read movie and rating data
movieDF = spark.read.options(delimiter=',').schema('movie_id INT, title STRING, genres STRING').csv("data/movies_small.csv")

# 2. Xuất số lượng phim thuộc về mỗi dòng phim được làm mỗi năm. Kết quả được sắp xếp theo năm
cau2 = movieDF.withColumn("Year", f.regexp_extract("title", r"\((\d{4})\)", 1))
cau2 = cau2.withColumn("Genre", f.explode(f.split("genres", "\|")))
result_cau2 = cau2.groupBy("Year").pivot("Genre").count()
result_cau2.show(100, truncate=False)       