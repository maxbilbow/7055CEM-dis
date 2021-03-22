
import os

from pyspark.ml.classification import LogisticRegression
from pyspark.sql import SparkSession, DataFrame
from processing.pre_process import PreProcess
from processing.test import get_accuracy

ROOT = r".."
DATA_IN = os.path.join(ROOT, "data") #Config.get("path.data.in"))
DATA_PROCESSED = os.path.join(ROOT, ".processed") #Config.get("path.data.processed"))
DATA_FOLDER = "smartphone-activity"
SOURCE_DATASET = os.path.join(DATA_IN, DATA_FOLDER ,"dataset.csv")
SOURCE_ATTRIBUTES = os.path.join(DATA_IN, DATA_FOLDER ,"attributes.csv")


def create_model(training_data: DataFrame):
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
    paramMap2 = {lr.probabilityCol: "probability"}  # type: ignore
    paramMapCombined = paramMap.copy()
    paramMapCombined.update(paramMap2)  # type: ignore

    # Now learn a new model using the paramMapCombined parameters.
    # paramMapCombined overrides all parameters set earlier via lr.set* methods.
    model2 = lr.fit(training_data, paramMapCombined)
    print("Model 2 was fit using parameters: ")
    model2.extractParamMap()
    return model2


def test_model(model: LogisticRegression, test_data: DataFrame):
    # Make predictions on test data using the Transformer.transform() method.
    # LogisticRegression.transform will only use the 'features' column.
    # Note that model2.transform() outputs a "myProbability" column instead of the usual
    # 'probability' column since we renamed the lr.probabilityCol parameter previously.
    prediction = model.transform(test_data)

    return get_accuracy(prediction)


def save_model(model, name="lrm"):

    path = os.path.join("..", "data", "models", DATA_FOLDER, "%s.model" % name)

    model.save(path)


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example: Reading CSV file without mentioning schema") \
        .getOrCreate()

    dataset = spark.read.load(SOURCE_DATASET, format="csv", sep=",", inferSchema=True, header=True)
    dataset.show(1)

    training_data, test_data = PreProcess.create_training_and_test_data(spark, dataset)

    model = create_model(training_data)
    accuracy = test_model(model, test_data)
    if accuracy > 0.9:
        save_model(model, "lrm")
        print("Model successfully saved")
    else:
        raise Exception("Model was less than 90% successful: %s%%" % accuracy)
