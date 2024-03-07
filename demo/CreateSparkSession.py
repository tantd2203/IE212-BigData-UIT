
import findspark;
findspark.init()

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField,StringType,IntegerType,StructType
spark = SparkSession.builder.appName("tanhehe").getOrCreate()
# RDD
# 
# spark.createDataFrame([("Java","2000"),("COde","2001")]).show()

# Create DataFrame from RDD
columms = ["language","users_count"]
data = ([("Java","2000"),("Python","2001")])
dfFromData2 = spark.createDataFrame(data).toDF(*columms)
# dfFromData2.show()

# DataFrame From List Collection
data2 =[("James","","Smith","3333","M",3000),("Jame","","Smit","333","F",3000),("James2","","Sm2ith","33233","M",3003)]

schema = StructType([
    \
    StructField("firstname",StringType(),True),\
    StructField("middlename",StringType(),True),\
    StructField("lastname",StringType(),True),\
    StructField("id",StringType(),True),\
    StructField("gender",StringType(),True),\
    StructField("salary",IntegerType(),True),\
])

df = spark.createDataFrame(data = data2,schema = schema)
# df.printSchema()
# df.show()

# DataFrame with CSV
transDF = spark.read.options(delimiter=',').schema(
    'trans_id INT, date STRING, cust_id INT, amount DOUBLE, game STRING, equiment STRING, city STRING, state STRING, mode STRING').csv(
    "data/trans.txt")

# transDF.printSchema()
# transDF.show()



# You have several way select columms


# transDF.select('cust_id', "amount").show()
# transDF.select(transDF.cust_id, transDF.amount).show()


# select from a list


# twocolumms = ["cust_id","amount"]
# transDF.select(twocolumms).show()