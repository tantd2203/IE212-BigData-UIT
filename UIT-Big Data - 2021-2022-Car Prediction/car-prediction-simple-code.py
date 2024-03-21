import pyspark
from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, IndexToString
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml import Pipeline

spark = SparkSession.builder.appName("CarPrediction").getOrCreate() 
spark = SparkSession.builder.getOrCreate()

# Load the data
train  = spark.read.csv("data/train.csv", header=True, inferSchema=True)
test = spark.read.csv("data/test.csv", header=True, inferSchema=True)

#  convert string to index
lableIndexer = StringIndexer(inputCol="acceptability", outputCol="label")   
buyIndexer = StringIndexer(inputCol="buying_price", outputCol="indexedBuyPrice")
indexedLabelTrain = lableIndexer.fit(train).transform(train)

df1 = buyIndexer.fit(indexedLabelTrain).transform(indexedLabelTrain)

# convert indexedByPrice to vector
assembler = VectorAssembler(inputCols=["indexedBuyPrice"],outputCol="features")
df2 = assembler.transform(df1)

#  Use the DecisionTreeClassifier to train the model
decission_tree_classifier_model2 = DecisionTreeClassifier(labelCol="label", featuresCol="features",maxDepth=10)
decission_tree_classifier_model2.fit(df2).transform(df2).show(5)

# create a pipeline
pipeline = Pipeline( stages = [buyIndexer, assembler,decission_tree_classifier_model2 ])


model = pipeline.fit(indexedLabelTrain)
model.transform(indexedLabelTrain)


#  Use the DecisionTreeClassifier to train the model
testSolutions = pipeline.fit(indexedLabelTrain).transform(test).select('car_id','prediction')
testSolutions.show()

labelsArray = ["unacc","acc","good","vgood"]
testSolutions = IndexToString(inputCol="prediction", outputCol="acceptability", labels = labelsArray).transform(testSolutions)
testSolutions.show()


# solutions = testSolutions.select('car_id','acceptability')
# solutions.show()
# solutions.toPandas().to_csv("dumpsolutions.csv",header=True, index=False)
