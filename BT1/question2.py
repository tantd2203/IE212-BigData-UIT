import pyspark
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
spark = SparkSession.builder.appName("tancute").getOrCreate()
from pyspark.sql.functions import col, month

custDF = spark.read.options(delimiter=',').schema(
    'cust_id INT, first_name STRING, last_name STRING, age INT, major STRING').csv("data/cust.txt")

transDF = spark.read.options(delimiter=',').schema(
    'trans_id INT, date STRING, cust_id INT, amount DOUBLE, game STRING, equiment STRING, city STRING, state STRING, mode STRING').csv(
    "data/trans.txt")


joined_df = custDF.join(transDF,on=['cust_id'], how='inner')
joined_df = joined_df.withColumn("month", F.date_format(F.to_date("date", "MM-dd-yyyy"), "MM"))

# Group by month and first_name, and sum the amount
result_df = joined_df.groupby("month", "first_name").agg(F.sum("amount").alias("total_amount"))

# Define a window specification partitioned by month and ordered by total_amount descending
window_spec = Window.partitionBy("month").orderBy(F.col("total_amount").desc())

# Rank the rows based on total_amount within each month
ranked_df = result_df.withColumn("rank", F.rank().over(window_spec))

# Filter the rows where rank is 1 (i.e., maximum total_amount for each month)
max_amount_per_month_df = ranked_df.filter(F.col("rank") == 1).drop("rank")

# Show the result
max_amount_per_month_df.show()