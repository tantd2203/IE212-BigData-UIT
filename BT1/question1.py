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

result_df = joined_df.groupBy("month").agg(
    F.sum("amount").alias("total amount"),
    F.concat(F.lit("["), F.concat_ws(", ", F.collect_set("first_name")), F.lit("]")).alias("unique_first_names")
).orderBy('month')

result_df.show(truncate=False)