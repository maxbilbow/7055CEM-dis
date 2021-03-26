import os
from abc import abstractmethod

from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.util import JavaMLWritable
from pyspark.sql import DataFrame

from config import Config
from repository.database import db
from typing import Union

MODELS_DIR = Config.get("path.data.models")
LABEL_COL = "activity"

class ClassificationModel:
    @abstractmethod
    def transform(self) -> DataFrame:
        pass


class ModelTrainer:
    def __init__(self, name):
        self.name = name

    @abstractmethod
    def create_model(self, training_data: DataFrame) -> Union[ClassificationModel, JavaMLWritable]:
        pass

    def test_model(self, model, test_data: DataFrame) -> float:
        # Make predictions on test data using the Transformer.transform() method.
        # LogisticRegression.transform will only use the 'features' column.
        prediction = model.transform(test_data)

        # obtain evaluator.
        evaluator = MulticlassClassificationEvaluator(metricName="accuracy", labelCol=LABEL_COL)

        # compute the classification error on test data.
        accuracy = evaluator.evaluate(prediction)
        print("Test Accuracy = %g" % accuracy)
        return accuracy

    def save_model(self, model: Union[ClassificationModel, JavaMLWritable]):
        path = os.path.join(MODELS_DIR, "{}.model".format(self.name))

        model.write().overwrite().save(path)

    def start(self):
        dataset = db.load_data_in()
        dataset.show(1)
        dataset.printSchema()

        training_data, test_data = ModelTrainer.create_training_and_test_data(dataset)

        model = self.create_model(training_data)
        accuracy = self.test_model(model, test_data)
        if accuracy > 0.9:
            self.save_model(model)
            print("Model successfully saved")
        else:
            raise Exception("Model was less than 90% successful: {}%".format(accuracy))

    @staticmethod
    def create_training_and_test_data(dataset: DataFrame):
        split = dataset.randomSplit([0.8, 0.2], 1234)
        train = split[0]
        test = split[1]

        print("Training: %s " % train.count())
        print("Test Data: %s " % test.count())
        return train, test
