from pyspark.ml.evaluation import MulticlassClassificationEvaluator

from repository.database import db


def start():
    prediction = db.load_results()
    prediction.show(1)

    # obtain evaluator.
    evaluator = MulticlassClassificationEvaluator(metricName="accuracy", labelCol="activity")

    # compute the classification error on test data.
    accuracy = evaluator.evaluate(prediction)
    print("Test Accuracy = {0:.2f}%".format(accuracy * 100))


if __name__ == "__main__":
    start()
