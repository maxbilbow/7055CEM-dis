import os

from pyspark.ml.classification import LogisticRegression
from pyspark.sql import DataFrame

from config import Config
from processing.pre_process import PreProcess
from processing.test import get_accuracy
import repository.database as db

ROOT = r".."
DATA_IN = os.path.join(ROOT, Config.get("path.data.in"))
SOURCE_DATASET = os.path.join(DATA_IN, "dataset.csv")
MODELS_DIR = os.path.join(ROOT, Config.get("path.data.models"))


def create_model(training_data: DataFrame):
    # Create a LogisticRegression instance. This instance is an Estimator.
    lr = LogisticRegression(maxIter=10, regParam=0.01, featuresCol="features", labelCol="activity")
    # Print out the parameters, documentation, and any default values.
    print("LogisticRegression parameters:\n" + lr.explainParams() + "\n")

    # Learn a LogisticRegression models. This uses the parameters stored in lr.
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

    # Now learn a new models using the paramMapCombined parameters.
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
    path = os.path.join(MODELS_DIR, "{}.model".format(name))

    model.save(path)


if __name__ == "__main__":
    spark = db.create_spark_session()

    dataset = spark.read.load(SOURCE_DATASET, format="csv", sep=",", inferSchema=True, header=True)
    dataset.show(1)

    training_data, test_data = PreProcess.create_training_and_test_data(dataset)

    model = create_model(training_data)
    accuracy = test_model(model, test_data)
    if accuracy > 0.9:
        save_model(model, "lrm")
        print("Model successfully saved")
    else:
        raise Exception("Model was less than 90% successful: %s%%" % accuracy)
