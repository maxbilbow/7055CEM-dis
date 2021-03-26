import os

from pyspark.ml.classification import LogisticRegression
from pyspark.sql import DataFrame

from config import Config

from processing.train_model import ModelTrainer

MODELS_DIR = Config.get("path.data.models")


class LrmTrainer(ModelTrainer):
    def __init__(self):
        super(LrmTrainer, self).__init__("lrm")

    def create_model(self, training_data: DataFrame):
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


def start():
    LrmTrainer().start()


if __name__ == "__main__":
    start()
