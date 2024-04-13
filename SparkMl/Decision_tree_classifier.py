from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql import SparkSession
# Create a SparkSession
spark = SparkSession.builder.appName("DecisionTreeClassifier").getOrCreate()

# Load the data stored in LIBSVM format as a DataFrame
# format  have two case: libsvm and image data 
data = spark.read.format("libsvm").load("SparkMl/dataset/sample_libsvm_data.txt")

# Display the data
# data.show(10)

# Index labels, adding metadata to the label column.
# Fit on whole dataset to include all labels in index.
# StringIndexer encodes a string column of labels to a column of label indices
# lableInderxer Model is fitted on the data and then transform the data
labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(data) 
lable_indexed = labelIndexer.transform(data)


# Automatically identify categorical features, and index them.
# We specify maxCategories so features with > 4 distinct values are treated as continuous.
featureIndexer =\
    VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=4).fit(data)

featureIndexer.transform(data)
# Split the data into training and test sets (30% held out for testing)

(trainingData, testData) = data.randomSplit([0.7, 0.3])
 
# Train a DecisionTree model.
# DecisionTree model is trained on the data and then transform the data
decision_tree_classifier_model = DecisionTreeClassifier(labelCol="indexedLabel", featuresCol="indexedFeatures")


# Chain indexers and tree in a Pipeline
pipeline = Pipeline(stages=[labelIndexer, featureIndexer, decision_tree_classifier_model])

# Train model.  This also runs the indexers.
model = pipeline.fit(trainingData)


# Make predictions.
predictions = model.transform(testData)

# Select example rows to display.
predictions.select("prediction", "indexedLabel", "features").show(5)


# Select (prediction, true label) and compute test error
evaluator = MulticlassClassificationEvaluator(
    labelCol="indexedLabel", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Test Error = %g " % (1.0 - accuracy))

treeModel = model.stages[2]
# summary only
print(treeModel)
