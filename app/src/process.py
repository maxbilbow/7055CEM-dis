import os

from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql import SparkSession
from pyspark.ml.classification import LogisticRegressionModel

from config import Config
from processing.pre_process import PreProcess

ROOT = r".."
DATA_IN = os.path.join(ROOT, Config.get("path.data.in"))
SOURCE_DATASET = os.path.join(DATA_IN, "dataset.csv")
MODELS_DIR = os.path.join(ROOT, Config.get("path.data.models"))


def test_model(test_data, model):
    prediction = model.transform(test_data)

    # obtain evaluator.
    evaluator = MulticlassClassificationEvaluator(metricName="accuracy", labelCol="activity")

    # compute the classification error on test data.
    return evaluator.evaluate(prediction)



def load_model(name="lrm"):
    path = os.path.join(MODELS_DIR, "{}.model".format(name))
    return LogisticRegressionModel.load(path)


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example: Reading CSV file without mentioning schema") \
        .getOrCreate()

    model = load_model()
    dataset = spark.read.load(SOURCE_DATASET, format="csv", sep=",", inferSchema=True, header=True)
    dataset = PreProcess.create_df_vector(dataset)
    dataset.show(1)
    accuracy = test_model(dataset, model)
    print("Model was {}% successful".format(accuracy))
