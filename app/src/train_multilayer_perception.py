import os

from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql import SparkSession, DataFrame
from processing.pre_process import PreProcess
from processing.test import get_accuracy

ROOT = r".."
DATA_IN = os.path.join(ROOT, "data")
DATA_FOLDER = "smartphone-activity"
SOURCE_DATASET = os.path.join(DATA_IN, DATA_FOLDER, "dataset.csv")
SOURCE_ATTRIBUTES = os.path.join(DATA_IN, DATA_FOLDER, "attributes.csv")
CLASSES = 6


def create_model(train: DataFrame, feature_col_names: list):
    # specify layers for the neural network:
    # input layer of size 4 (features), two intermediate of size 5 and 4
    # and output of size 3 (classes)
    layers = [len(feature_col_names), 10, 8, CLASSES]

    # create the trainer and set its parameters
    trainer = MultilayerPerceptronClassifier(maxIter=100, layers=layers, blockSize=128, seed=1234,
                                             featuresCol="features", labelCol="activity")

    # train the model
    return trainer.fit(train)


def test_model(model, test_data: DataFrame):
    # Make predictions on test data using the Transformer.transform() method.
    # LogisticRegression.transform will only use the 'features' column.
    # Note that model2.transform() outputs a "myProbability" column instead of the usual
    # 'probability' column since we renamed the lr.probabilityCol parameter previously.
    prediction = model.transform(test_data)

    predictionAndLabels = prediction.select("prediction", "activity")
    evaluator = MulticlassClassificationEvaluator(metricName="accuracy")
    acc = evaluator.evaluate(predictionAndLabels)
    print("Test set accuracy = " + str(accuracy))
    return acc


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

    features = PreProcess.get_feature_column_names(dataset)
    training_data, test_data = PreProcess.create_training_and_test_data(dataset, features)

    model = create_model(training_data, features)
    accuracy = test_model(model, test_data)
    if accuracy > 0.9:
        save_model(model, "mpc")
        print("Model successfully saved")
    else:
        raise Exception("Model was less than 90%% successful: %s%%" % accuracy)
