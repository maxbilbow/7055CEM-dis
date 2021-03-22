
import os

ROOT = r".."
DATA_IN = os.path.join(ROOT, "data") #Config.get("path.data.in"))
DATA_PROCESSED = os.path.join(ROOT, ".processed") #Config.get("path.data.processed"))
DATA_FOLDER = "smartphone-activity"
SOURCE_DATASET = os.path.join(DATA_IN, DATA_FOLDER ,"dataset.csv")
SOURCE_ATTRIBUTES = os.path.join(DATA_IN, DATA_FOLDER ,"attributes.csv")

from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example: Reading CSV file without mentioning schema") \
    .getOrCreate()
dataset = spark.read.load(SOURCE_DATASET, format="csv", sep=",", inferSchema=True, header=True)
dataset.show(10)
attrs = spark.read.csv(SOURCE_ATTRIBUTES, header=True)
attrs.show(10)

print("Columns: %s" % attrs.select("name").count())

from processing.feature_reduction import find_correlation

def reduce_feature_set(df=dataset):
    pandas_df = df.toPandas()
    print("All Features: %s" % len(pandas_df.columns))
    reduced_feature_set = find_correlation(pandas_df)
    print("Reduced Features: %s" % len(reduced_feature_set))
    return list(reduced_feature_set)

# reduce_feature_set()

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression

VECTOR_COL = "features"

def create_df_vector(df=dataset, columns: list = reduce_feature_set()):
    assembler = VectorAssembler(
        inputCols=columns,
        outputCol=VECTOR_COL)

    df_vector = assembler.transform(df)
    df_vector = df_vector.select(*[VECTOR_COL, "activity"])
    return df_vector

create_df_vector().show(5)

import numpy as np

def create_training_and_test_data():
    df = create_df_vector(dataset).toPandas()
    msk = np.random.rand(len(df)) < 0.8

    train = spark.createDataFrame(df[msk])
    # training_data = spark.createDataFrame(training_data)
    # training_data = create_df_vector(training_data)

    test = spark.createDataFrame(df[~msk])
    # test_data = spark.createDataFrame(test_data)
    # test_data = create_df_vector(test_data)

    print("Training: %s " % train.count())
    print("Test Data: %s " % test.count())
    return train, test

# create_training_and_test_data()

training_data, test_data = create_training_and_test_data()
# Create a LogisticRegression instance. This instance is an Estimator.
lr = LogisticRegression(maxIter=10, regParam=0.01, featuresCol="features", labelCol="activity")
# Print out the parameters, documentation, and any default values.
print("LogisticRegression parameters:\n" + lr.explainParams() + "\n")

# Learn a LogisticRegression model. This uses the parameters stored in lr.
model1 = lr.fit(training_data)

print("Model 1 was fit using parameters: ")
model1.extractParamMap()

# We may alternatively specify parameters using a Python dictionary as a paramMap
paramMap = {lr.maxIter: 20}
paramMap[lr.maxIter] = 30  # Specify 1 Param, overwriting the original maxIter.
# Specify multiple Params.
paramMap.update({lr.regParam: 0.1, lr.threshold: 0.55})  # type: ignore

# You can combine paramMaps, which are python dictionaries.
# Change output column name
paramMap2 = {lr.probabilityCol: "myProbability"}  # type: ignore
paramMapCombined = paramMap.copy()
paramMapCombined.update(paramMap2)  # type: ignore

# Now learn a new model using the paramMapCombined parameters.
# paramMapCombined overrides all parameters set earlier via lr.set* methods.
model2 = lr.fit(training_data, paramMapCombined)
print("Model 2 was fit using parameters: ")
model2.extractParamMap()


# Prepare test data

# Make predictions on test data using the Transformer.transform() method.
# LogisticRegression.transform will only use the 'features' column.
# Note that model2.transform() outputs a "myProbability" column instead of the usual
# 'probability' column since we renamed the lr.probabilityCol parameter previously.
prediction = model2.transform(test_data)
result = prediction.select("features", "activity", "myProbability", "prediction") \
    .collect()

right = wrong = 0
for row in result:
    if row.activity == row.prediction:
        right+=1
    else:
        wrong+=1

total = right + wrong
percent = round(right * 100 / total)
print("Accuracy: %s/%s (%s%%)" % (right , total, percent))
# for row in result:
#     print("features=%s, activity=%s -> prob=%s, prediction=%s"
#           % (row.features, row.activity, row.myProbability, row.prediction))

# MODEL1_PATH = os.path.join("..", "data", "models", DATA_FOLDER, "lrm1.model")
MODEL2_PATH = os.path.join("..", "data", "models", DATA_FOLDER, "lrm.model")

# model1.save(MODEL1_PATH)
model2.save(MODEL2_PATH)



