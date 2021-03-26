import os

from pyspark.ml.classification import LogisticRegressionModel
from config import Config
from repository.database import db

MODELS_DIR = Config.get("path.data.models")


def load_model(name="lrm"):
    path = os.path.join(MODELS_DIR, "{}.model".format(name))
    return LogisticRegressionModel.load(path)


def start():
    dataset = db.load_data_in()
    model = load_model()
    dataset.show(1)
    prediction = model.transform(dataset)
    prediction.show(1)
    db.write_to_results(prediction)


if __name__ == "__main__":
    start()
